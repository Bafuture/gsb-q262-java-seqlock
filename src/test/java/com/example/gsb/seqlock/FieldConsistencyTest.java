package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** 复合结构一致性：一次读取多个字段必须整体来自同一版本，不允许撕裂。 */
class FieldConsistencyTest {

    @Test
    void snapshotContainsAllFieldsOfOneVersion() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        lock.write(d -> d.set(123_456, PairData.SUM - 123_456));

        PairData snapshot = lock.read(PairData::new);

        assertThat(snapshot.getA()).isEqualTo(123_456);
        assertThat(snapshot.getB()).isEqualTo(PairData.SUM - 123_456);
        assertThat(snapshot.satisfiesInvariant()).isTrue();
    }

    /**
     * 确定性撕裂场景：第一次复制在拷贝完 a 之后、拷贝 b 之前，让另一个线程完成一次写入。
     * 读者必须发现版本变化、丢弃撕裂结果并重试，最终返回新版本的完整快照。
     */
    @Test
    void tornCopyIsDetectedAndRetried() throws InterruptedException {
        PairData data = new PairData(0, PairData.SUM);
        CountDownLatch startWrite = new CountDownLatch(1);
        CountDownLatch writeDone = new CountDownLatch(1);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);

        Seqlock<PairData> lock = new Seqlock<>(data, (src, dst) -> {
            dst.setA(src.getA());
            if (firstAttempt.compareAndSet(true, false)) {
                startWrite.countDown();
                await(writeDone); // 复制中途插入一次完整写入，制造撕裂
            }
            dst.setB(src.getB());
        }, 100, OverflowPolicy.THROW);

        Thread writer = new Thread(() -> {
            await(startWrite);
            lock.write(d -> d.set(42, PairData.SUM - 42));
            writeDone.countDown();
        });
        writer.start();

        PairData snapshot = lock.read(new PairData());
        writer.join(5_000);

        // 撕裂的中间结果 (a=0, b=SUM-42) 绝不能被返回；必须重试到一致的新版本
        assertThat(snapshot.getA()).isEqualTo(42);
        assertThat(snapshot.getB()).isEqualTo(PairData.SUM - 42);
        assertThat(snapshot.satisfiesInvariant()).isTrue();
        assertThat(lock.stats().retries()).isEqualTo(1);
        assertThat(lock.sequence()).isEqualTo(2);
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
}
