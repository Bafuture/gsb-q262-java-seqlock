package com.example.gsb.seqlock;

/** {@link Seqlock#tryRead} 自旋重试超过上限时抛出。 */
public final class RetryLimitExceededException extends RuntimeException {

    private final int maxRetries;

    public RetryLimitExceededException(int maxRetries) {
        super("seqlock optimistic read failed after " + maxRetries + " retries");
        this.maxRetries = maxRetries;
    }

    public int maxRetries() {
        return maxRetries;
    }
}
