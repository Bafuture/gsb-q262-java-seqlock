package com.example.gsb.seqlock;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 序列锁（Sequence Lock）：面向"读多写少、写入很短、读取需要一致快照"的场景。
 *
 * <h3>版本号协议</h3>
 * <ul>
 *   <li>版本号为偶数：数据稳定，可读；为奇数：写者正在修改。</li>
 *   <li>写者进入时版本 +1（偶 → 奇），写完再 +1（奇 → 偶）。</li>
 *   <li>读者在读前、读后各取一次版本，二者一致且为偶数则快照有效，否则重试。</li>
 * </ul>
 *
 * <h3>写者优先，为什么不需要读写互斥</h3>
 * 读者<b>不持有任何锁</b>，只做"读版本 → 读数据 → 校验版本"的乐观尝试，
 * 失败就重试，因此读者永远不会阻塞写者，写者天然优先。
 * 唯一的互斥是写者之间的互斥（{@link #writeLock}），因为版本协议只保证
 * "一个写者 vs 多个读者"的正确性，多个写者并发修改同一数据仍必须串行化。
 * 读者与写者之间不需要互斥：读者检测到并发写（版本变奇或前后不一致）后
 * 丢弃本次结果重读即可，正确性由版本校验而非锁来保证。
 *
 * <h3>内存语义</h3>
 * 写者：奇数版本用 volatile 写发布"开始修改"，偶数版本用 release 写发布
 * "修改完成"，保证数据写入先于偶数版本对其他线程可见。
 * 读者：两次版本读取均为 acquire 读，保证读到偶数版本后再读数据，
 * 能看到写者在该版本之前写入的全部数据。因此被保护的数据字段可以是普通字段。
 *
 * <h3>读者饥饿与缓解</h3>
 * 写者长时间写入（或写过于频繁）时，读者可能反复校验失败而空转（饥饿）。
 * 缓解策略：自旋达到 {@link #maxRetries} 次后，{@link #read} 退化为
 * 悲观加锁读（获取写锁后读取），以阻塞换确定的进展；{@link #tryRead}
 * 则直接抛出 {@link RetryLimitExceededException}，由调用方决定降级逻辑。
 */
public final class Seqlock {

    private static final VarHandle VERSION;

    static {
        try {
            VERSION = MethodHandles.lookup().findVarHandle(Seqlock.class, "version", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** 默认读者最大自旋重试次数。 */
    public static final int DEFAULT_MAX_RETRIES = 1024;

    /** 版本号：偶数 = 稳定，奇数 = 写者正在修改。仅写者线程修改（持写锁）。 */
    private volatile long version;

    /** 仅用于写者之间的互斥；读者从不获取它（除非自旋超限后降级）。 */
    private final ReentrantLock writeLock = new ReentrantLock();

    private final int maxRetries;

    private final LongAdder readCount = new LongAdder();
    private final LongAdder retryCount = new LongAdder();
    private final AtomicLong maxRetriesObserved = new AtomicLong();
    private final LongAdder fallbackCount = new LongAdder();

    public Seqlock() {
        this(DEFAULT_MAX_RETRIES);
    }

    /**
     * @param maxRetries 读者自旋重试上限（不含第一次尝试），达到上限后
     *                   {@link #read} 降级为加锁读，{@link #tryRead} 抛异常
     */
    public Seqlock(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0: " + maxRetries);
        }
        this.maxRetries = maxRetries;
    }

    /** 当前版本号（acquire 语义）。偶数 = 稳定，奇数 = 写者正在修改。 */
    public long version() {
        return (long) VERSION.getAcquire(this);
    }

    /** 读者最大自旋重试上限。 */
    public int maxRetries() {
        return maxRetries;
    }

    /**
     * 写操作。写者之间互斥；读者不会阻塞写者。
     * 进入时版本 +1（变奇），写完（含异常）再 +1（变偶）。
     */
    public void write(Runnable action) {
        writeLock.lock();
        try {
            long before = (long) VERSION.getAndAdd(this, 1L); // 偶 -> 奇，volatile 全栅栏
            try {
                action.run();
            } finally {
                VERSION.setRelease(this, before + 2L); // 奇 -> 偶，release 发布数据
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 乐观读：自旋重试直到读到一致快照；超过重试上限后降级为加锁读，
     * 保证写者长时间写入时读者最终能完成（缓解读者饥饿）。
     *
     * @param reader 读取被保护字段并组装快照，必须是无副作用的纯读取
     */
    public <T> T read(Supplier<T> reader) {
        int retries = 0;
        for (;;) {
            long before = (long) VERSION.getAcquire(this);
            if ((before & 1L) == 0L) {
                T result = reader.get();
                long after = (long) VERSION.getAcquire(this);
                if (after == before) {
                    recordSuccess(retries);
                    return result;
                }
            }
            if (retries >= maxRetries) {
                return lockedRead(reader, retries);
            }
            retries++;
            Thread.onSpinWait();
        }
    }

    /**
     * 乐观读：超过重试上限直接抛 {@link RetryLimitExceededException}，
     * 不降级加锁，适合不允许阻塞的调用方。
     */
    public <T> T tryRead(Supplier<T> reader) {
        int retries = 0;
        for (;;) {
            long before = (long) VERSION.getAcquire(this);
            if ((before & 1L) == 0L) {
                T result = reader.get();
                long after = (long) VERSION.getAcquire(this);
                if (after == before) {
                    recordSuccess(retries);
                    return result;
                }
            }
            if (retries >= maxRetries) {
                recordFailure(retries);
                throw new RetryLimitExceededException(maxRetries);
            }
            retries++;
            Thread.onSpinWait();
        }
    }

    /** 自旋超限后的兜底：获取写锁读取，与写者互斥，保证进展。 */
    private <T> T lockedRead(Supplier<T> reader, int retries) {
        fallbackCount.increment();
        writeLock.lock();
        try {
            T result = reader.get();
            recordSuccess(retries);
            return result;
        } finally {
            writeLock.unlock();
        }
    }

    private void recordSuccess(int retries) {
        readCount.increment();
        recordRetries(retries);
    }

    private void recordFailure(int retries) {
        recordRetries(retries);
    }

    private void recordRetries(int retries) {
        if (retries > 0) {
            retryCount.add(retries);
            maxRetriesObserved.accumulateAndGet(retries, Math::max);
        }
    }

    /** 统计快照。 */
    public SeqlockStats stats() {
        return new SeqlockStats(
                readCount.sum(),
                retryCount.sum(),
                maxRetriesObserved.get(),
                fallbackCount.sum());
    }
}
