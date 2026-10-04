package com.example.gsb.seqlock;

import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 序列锁（Seqlock）。
 *
 * <p>读多写少场景下替代读写锁的无等待（读者侧）同步原语：</p>
 * <ul>
 *   <li>读者完全不加锁：读前记录版本号，整体复制受保护数据，读后再读版本号；
 *       版本为偶数且前后相等时快照才有效，否则自旋重试。</li>
 *   <li>写者只与其它写者通过一把互斥锁串行化，从不等待读者，因此写者优先、永不被读者阻塞。</li>
 * </ul>
 *
 * <h2>版本号协议</h2>
 * <pre>
 *   写者：lock() → seq++（变奇，标记“写入中”）→ 修改字段 → seq++（变偶，提交新版本）→ unlock()
 *   读者：seq1 = seq；若 seq1 为奇数则重试；复制全部字段；seq2 = seq；
 *         当且仅当 seq1 == seq2（均为偶数）时快照一致，否则重试。
 * </pre>
 *
 * <h2>Java 内存模型下为什么正确</h2>
 * <p>{@code sequence} 是 {@code volatile}：</p>
 * <ul>
 *   <li>写者第二次 {@code volatile} 写（发布偶数版本）具有 release 语义，
 *       保证之前对普通字段的写入对看到该版本号的读者可见；</li>
 *   <li>读者两次 {@code volatile} 读具有 acquire 语义，保证字段读不会被提升到第一次
 *       版本读之前；但 JMM 并不禁止字段读被<em>下移</em>到第二次版本读之后（volatile 读
 *       只挡后续操作前移），因此复制与第二次版本读之间必须插入
 *       {@link VarHandle#loadFence()}（x86 上仅是编译器屏障，零运行时开销），
 *       把复制动作严格夹在两次版本校验之间；</li>
 *   <li>复制期间若有写者开始：版本变为奇数；若写者恰好完整完成一次写入：版本加 2。
 *       两种情况 {@code seq1 != seq2}，读者丢弃结果并重试，因此多字段不可能跨版本撕裂。</li>
 * </ul>
 *
 * <h2>为什么读写之间不需要互斥</h2>
 * <p>读者对受保护数据的读取可能与单个写者并发（读到中间态），但版本号使这种中间态
 * <em>必然被检测到并丢弃</em>。读者从不修改数据，不会破坏写者；写者的中间态也永远不会
 * 作为有效快照返回。因此读者与写者之间没有临界区需要互斥；互斥只存在于多个写者之间，
 * 用来保证 {@code seq++ / 修改 / seq++} 序列不被另一个写者穿插。</p>
 *
 * <h2>读者饥饿与缓解</h2>
 * <p>若写者临界区很长、或写者源源不断，读者可能持续重试而饥饿。本实现提供：</p>
 * <ol>
 *   <li>有限重试：默认 {@link #DEFAULT_MAX_RETRIES} 轮，超过后按 {@link OverflowPolicy}
 *       处理，避免无界自旋烧 CPU；</li>
 *   <li>退避：{@link Thread#onSpinWait()}（前若干轮）→ {@link Thread#yield()} →
 *       {@link LockSupport#parkNanos} 有界指数退避，降低缓存一致性流量、把 CPU 让给写者；</li>
 *   <li>{@link OverflowPolicy#BLOCK}：读者退化为竞争一把公平写者锁，保证最终一定能读到
 *       （以可能阻塞为代价）；</li>
 *   <li>使用建议：尽量缩短写临界区、合并写入、写者侧保持单写者/少写者。</li>
 * </ol>
 *
 * @param <T> 受保护的复合数据类型；字段读写通过构造时提供的 copier 完成
 */
public final class Seqlock<T> {

    /** 默认重试上限（重试轮数，不含第一次尝试）。 */
    public static final int DEFAULT_MAX_RETRIES = 1024;

    private final T data;
    private final BiConsumer<? super T, ? super T> copier;
    private final int maxRetries;
    private final OverflowPolicy overflowPolicy;

    /** 偶数 = 稳定版本，奇数 = 写者临界区进行中。 */
    private volatile int sequence;

    /** 仅用于写者之间互斥；读者从不获取它（BLOCK 兜底路径除外）。公平排队避免兜底读者饿死。 */
    private final ReentrantLock writeMutex = new ReentrantLock(true);

    private final SeqlockStats stats = new SeqlockStats();

    /**
     * 使用默认重试上限 {@link #DEFAULT_MAX_RETRIES} 与 {@link OverflowPolicy#THROW} 策略。
     *
     * @param data   受保护数据实例（原地修改）
     * @param copier 将源快照完整复制到目标缓冲的回调 {@code (from, to) -> ...}，
     *               必须复制读者需要的全部字段
     */
    public Seqlock(T data, BiConsumer<? super T, ? super T> copier) {
        this(data, copier, DEFAULT_MAX_RETRIES, OverflowPolicy.THROW);
    }

    /**
     * @param maxRetries      重试上限（≥0）：读到不一致后最多再试多少轮
     * @param overflowPolicy  超限处理：{@link OverflowPolicy#THROW} 或 {@link OverflowPolicy#BLOCK}
     */
    public Seqlock(T data,
                   BiConsumer<? super T, ? super T> copier,
                   int maxRetries,
                   OverflowPolicy overflowPolicy) {
        this.data = Objects.requireNonNull(data, "data");
        this.copier = Objects.requireNonNull(copier, "copier");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, was " + maxRetries);
        }
        this.maxRetries = maxRetries;
        this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy");
    }

    /**
     * 在写者临界区内原地修改受保护数据。写者只与其它写者串行，永不等待读者。
     */
    public void write(Consumer<? super T> mutation) {
        Objects.requireNonNull(mutation, "mutation");
        writeMutex.lock();
        try {
            sequence++;            // 变奇：读者遇到奇数版本立即重试
            mutation.accept(data);
            sequence++;            // 变偶：发布新版本（volatile 写，release）
        } finally {
            writeMutex.unlock();
        }
    }

    /**
     * 读取一致快照到调用方提供的缓冲中。
     *
     * <p>可复用同一个缓冲对象，读取路径零额外分配。</p>
     *
     * @param into 接收快照的缓冲
     * @return 与传入相同的缓冲实例
     * @throws RetryLimitExceededException 超过重试上限且策略为 THROW
     */
    public T read(T into) {
        Objects.requireNonNull(into, "into");
        stats.onReadCall();
        int retries = 0;
        while (true) {
            stats.onAttempt();
            int before = sequence;                 // volatile 读（acquire）
            if ((before & 1) == 0) {
                copier.accept(data, into);         // 复制全部字段（普通读，夹在两次版本读之间）
                VarHandle.loadLoadFence();        // 关键：禁止字段读被 JIT 下移到第二次版本读之后
                int after = sequence;              // volatile 读（acquire）
                if (before == after) {
                    stats.onReadFinished(retries);
                    return into;
                }
            }
            // 版本为奇数（写入中）或前后不等（复制期间发生过写入）→ 丢弃本轮，重试
            retries++;
            if (retries > maxRetries) {
                stats.onReadFinished(retries);
                return overflow(into, retries);
            }
            backoff(retries);
        }
    }

    /**
     * 读取一致快照，每轮复制到新分配的缓冲中。
     */
    public T read(Supplier<? extends T> bufferFactory) {
        Objects.requireNonNull(bufferFactory, "bufferFactory");
        return read(bufferFactory.get());
    }

    /**
     * 重试超限后的处理：THROW 快速失败，或 BLOCK 获取写者互斥锁兜底复制。
     *
     * <p>持锁期间任何写者都不可能处于临界区（写者先拿锁才会翻版本号），
     * 因此无需再校验版本即可得到一致快照。</p>
     */
    private T overflow(T into, int retries) {
        if (overflowPolicy == OverflowPolicy.THROW) {
            throw new RetryLimitExceededException(retries, maxRetries);
        }
        stats.onFallback();
        writeMutex.lock();
        try {
            copier.accept(data, into);
            return into;
        } finally {
            writeMutex.unlock();
        }
    }

    /** 有界自旋退避，缓解持续争用下的读者饥饿与总线风暴。 */
    private static void backoff(int retry) {
        if (retry <= 16) {
            Thread.onSpinWait();
        } else if (retry <= 64) {
            Thread.yield();
        } else {
            long nanos = 1_000L << Math.min(retry - 64, 6); // 1µs 起，上限 64µs
            LockSupport.parkNanos(nanos);
        }
    }

    /** 当前版本号（偶数为稳定版本，奇数表示写入中），主要用于诊断与测试。 */
    public int sequence() {
        return sequence;
    }

    /** 当前是否有写者处于临界区。 */
    public boolean writeInProgress() {
        return (sequence & 1) != 0;
    }

    /** 读取侧统计。 */
    public SeqlockStats stats() {
        return stats;
    }
}
