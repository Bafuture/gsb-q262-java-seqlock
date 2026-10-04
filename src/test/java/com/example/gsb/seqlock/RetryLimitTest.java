package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 读者自旋重试的上限与超限处理（THROW 快速失败 / BLOCK 兜底阻塞）。 */
class RetryLimitTest {

    @Test
    void rejectsNegativeRetryLimit() {
        assertThatThrownBy(() -> new Seqlock<>(new PairData(), PairData::copyTo, -1, OverflowPolicy.THROW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwPolicyFailsFastWhenWriterHoldsTooLong() throws InterruptedException {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo, 3, OverflowPolicy.THROW);
        CountDownLatch writerInside = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            lock.write(d -> {
                writerInside.countDown();
                await(releaseWriter); // 模拟写者长时间写入
                d.set(7, PairData.SUM - 7);
            });
        });
        writer.start();
        await(writerInside);

        assertThatThrownBy(() -> lock.read(new PairData()))
                .isInstanceOfSatisfying(RetryLimitExceededException.class, e -> {
                    assertThat(e.maxRetries()).isEqualTo(3);
                    assertThat(e.retries()).isGreaterThan(3);
                });

        releaseWriter.countDown();
        writer.join(5_000);

        // 写者结束后读取立即恢复
        PairData snapshot = lock.read(new PairData());
        assertThat(snapshot.getA()).isEqualTo(7);
        assertThat(snapshot.satisfiesInvariant()).isTrue();
    }

    @Test
    void zeroRetryLimitOverflowsOnFirstInconsistency() throws InterruptedException {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo, 0, OverflowPolicy.THROW);
        CountDownLatch writerInside = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            lock.write(d -> {
                writerInside.countDown();
                await(releaseWriter);
            });
        });
        writer.start();
        await(writerInside);

        assertThatThrownBy(() -> lock.read(new PairData()))
                .isInstanceOf(RetryLimitExceededException.class);

        releaseWriter.countDown();
        writer.join(5_000);
    }

    @Test
    void blockPolicyFallsBackToWriterMutexAndEventuallyReads() throws InterruptedException {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo, 3, OverflowPolicy.BLOCK);
        CountDownLatch writerInside = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            lock.write(d -> {
                writerInside.countDown();
                sleep(150); // 写者长时间持有临界区
                d.set(9, PairData.SUM - 9);
            });
        });
        writer.start();
        await(writerInside);

        // 读者自旋 3 次后超限，退化为竞争写者互斥锁，阻塞等待写者完成后拿到一致快照
        PairData snapshot = lock.read(new PairData());
        writer.join(5_000);

        assertThat(snapshot.getA()).isEqualTo(9);
        assertThat(snapshot.getB()).isEqualTo(PairData.SUM - 9);
        assertThat(snapshot.satisfiesInvariant()).isTrue();
        assertThat(lock.stats().fallbackReads()).isEqualTo(1);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
