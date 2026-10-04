package com.example.gsb.seqlock;

/**
 * Seqlock 运行统计的不可变快照。
 *
 * @param reads       成功完成的读取次数（含降级加锁读）
 * @param retries     所有读取累计的自旋重试次数
 * @param maxRetries  单次读取出现过的最大重试次数
 * @param fallbacks   自旋超限后降级为加锁读的次数
 */
public record SeqlockStats(long reads, long retries, long maxRetries, long fallbacks) {

    /** 重试率 = 累计重试次数 / 成功读取次数；无读取时为 0。 */
    public double retryRate() {
        return reads == 0L ? 0.0d : (double) retries / (double) reads;
    }
}
