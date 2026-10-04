package com.example.gsb.seqlock;

/**
 * 读者自旋重试次数超过上限（且策略为 {@link OverflowPolicy#THROW}）时抛出。
 */
public final class RetryLimitExceededException extends RuntimeException {

    private final int retries;
    private final int maxRetries;

    public RetryLimitExceededException(int retries, int maxRetries) {
        super("Seqlock read exceeded retry limit: retried " + retries
                + " times (max " + maxRetries + "). "
                + "Writer may be slow or writing continuously; "
                + "consider OverflowPolicy.BLOCK or shortening the write section.");
        this.retries = retries;
        this.maxRetries = maxRetries;
    }

    /** 本次读取实际执行的重试次数。 */
    public int retries() {
        return retries;
    }

    /** 配置的重试上限。 */
    public int maxRetries() {
        return maxRetries;
    }
}
