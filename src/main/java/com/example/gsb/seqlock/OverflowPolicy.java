package com.example.gsb.seqlock;

/**
 * 读者自旋重试达到上限后的处理策略。
 */
public enum OverflowPolicy {

    /**
     * 快速失败：抛出 {@link RetryLimitExceededException}，由调用方决定稍后重试或降级处理。
     * 不会因为慢速写者而阻塞读者线程。
     */
    THROW,

    /**
     * 阻塞兜底：读者退化为竞争写者互斥锁（公平锁）。拿到锁期间不可能有写者在临界区内，
     * 因此可以直接复制出一致快照。代价是读者会阻塞（与写者排队），但保证最终一定能读到，
     * 用于缓解“写者长时间写入 / 写者连续不断”时的读者饥饿。
     */
    BLOCK
}
