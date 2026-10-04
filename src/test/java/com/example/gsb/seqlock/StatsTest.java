package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** 统计：读取次数、重试次数、重试率、最大重试次数（以及重置与快照）。 */
class StatsTest {

    @Test
    void uncontendedReadHasZeroRetries() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        lock.write(d -> d.set(5, PairData.SUM - 5));

        lock.read(new PairData());
        lock.read(new PairData());

        SeqlockStats stats = lock.stats();
        assertThat(stats.readCalls()).isEqualTo(2);
        assertThat(stats.attempts()).isEqualTo(2);
        assertThat(stats.retries()).isZero();
        assertThat(stats.retryRate()).isZero();
        assertThat(stats.maxRetriesPerRead()).isZero();
        assertThat(stats.fallbackReads()).isZero();
    }

    @Test
    void retryCountersReflectDeterministicContention() {
        PairData data = new PairData(0, PairData.SUM);
        CountDownLatch startWrite = new CountDownLatch(1);
        CountDownLatch writeDone = new CountDownLatch(1);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);

        Seqlock<PairData> lock = new Seqlock<>(data, (src, dst) -> {
            dst.setA(src.getA());
            if (firstAttempt.compareAndSet(true, false)) {
                startWrite.countDown();
                await(writeDone);
            }
            dst.setB(src.getB());
        }, 100, OverflowPolicy.THROW);

        Thread writer = new Thread(() -> {
            await(startWrite);
            lock.write(d -> d.set(42, PairData.SUM - 42));
            writeDone.countDown();
        });
        writer.start();

        lock.read(new PairData());
        awaitThread(writer);

        SeqlockStats stats = lock.stats();
        assertThat(stats.readCalls()).isEqualTo(1);
        assertThat(stats.attempts()).isEqualTo(2);
        assertThat(stats.retries()).isEqualTo(1);
        assertThat(stats.retryRate()).isEqualTo(0.5, org.assertj.core.data.Offset.offset(1e-12));
        assertThat(stats.maxRetriesPerRead()).isEqualTo(1);
    }

    @Test
    void snapshotAndResetWork() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);

        lock.read(new PairData());
        SeqlockStats.Snapshot before = lock.stats().snapshot();
        assertThat(before.readCalls()).isEqualTo(1);
        assertThat(bottomEquals(before)).isTrue();

        lock.stats().reset();
        assertThat(lock.stats().snapshot()).isEqualTo(new SeqlockStats.Snapshot(0, 0, 0, 0.0, 0, 0));
    }

    private static boolean bottomEquals(SeqlockStats.Snapshot s) {
        return s.attempts() == 1 && s.retries() == 0
                && s.retryRate() == 0.0 && s.maxRetriesPerRead() == 0 && s.fallbackReads() == 0;
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

    private static void awaitThread(Thread t) {
        try {
            t.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
