package com.example.gsb.seqlock;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Seqlock 读取侧统计计数器（线程安全）。
 *
 * <p>计数口径：</p>
 * <ul>
 *   <li>{@code readCalls}：调用 {@code read} 的总次数（一次调用算一次读取，无论重试多少轮）。</li>
 *   <li>{@code attempts}：读取尝试总轮数。第一轮算尝试，每次重试再加一，
 *       因此 {@code retries = attempts - readCalls}。</li>
 *   <li>{@code retryRate}：重试率，定义为 {@code retries / attempts}，取值 [0, 1)。</li>
 *   <li>{@code maxRetriesPerRead}：单次读取在成功/失败前经历过的最大重试轮数。</li>
 *   <li>{@code fallbackReads}：重试超限后通过 {@link OverflowPolicy#BLOCK} 兜底成功的次数。</li>
 * </ul>
 */
public final class SeqlockStats {

    /** 某一时刻统计数据的不可变快照。 */
    public record Snapshot(long readCalls,
                           long attempts,
                           long retries,
                           double retryRate,
                           long maxRetriesPerRead,
                           long fallbackReads) {
    }

    private final LongAdder readCalls = new LongAdder();
    private final LongAdder attempts = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private final LongAccumulator maxRetries = new LongAccumulator(Long::max, 0L);

    void onReadCall() {
        readCalls.increment();
    }

    void onAttempt() {
        attempts.increment();
    }

    void onReadFinished(int retries) {
        if (retries > 0) {
            maxRetries.accumulate(retries);
        }
    }

    void onFallback() {
        fallbacks.increment();
    }

    /** 读取调用总次数。 */
    public long readCalls() {
        return readCalls.sum();
    }

    /** 读取尝试总轮数（含第一轮）。 */
    public long attempts() {
        return attempts.sum();
    }

    /** 重试总次数。 */
    public long retries() {
        return attempts.sum() - readCalls.sum();
    }

    /** 重试率：重试次数 / 尝试总轮数。没有任何读取时返回 0。 */
    public double retryRate() {
        long totalAttempts = attempts.sum();
        if (totalAttempts == 0) {
            return 0.0;
        }
        return (double) (totalAttempts - readCalls.sum()) / totalAttempts;
    }

    /** 单次读取的最大重试轮数。 */
    public long maxRetriesPerRead() {
        return maxRetries.get();
    }

    /** 重试超限后走 BLOCK 兜底成功的次数。 */
    public long fallbackReads() {
        return fallbacks.sum();
    }

    /** 当前统计的不可变快照。 */
    public Snapshot snapshot() {
        return new Snapshot(readCalls(), attempts(), retries(),
                retryRate(), maxRetriesPerRead(), fallbackReads());
    }

    /** 清零所有计数器（主要用于测试）。 */
    public void reset() {
        readCalls.reset();
        attempts.reset();
        fallbacks.reset();
        maxRetries.reset();
    }
}
