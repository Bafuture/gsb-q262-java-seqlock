package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * 并发压测：多个写者持续修改 (a, b)（维持 a + b == SUM），多个读者持续读快照。
 * 读到的任何一个快照都必须满足不变式，否则说明发生了跨版本撕裂。
 */
class ConcurrentNoTearStressTest {

    private static final long DURATION_MS = 2_500;

    @Test
    void concurrentReadsAlwaysSatisfyInvariant() throws Exception {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        int writerCount = 2;
        int readerCount = 4;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(writerCount + readerCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < writerCount; w++) {
            futures.add(pool.submit(() -> writerLoop(lock, failure)));
        }
        for (int r = 0; r < readerCount; r++) {
            final boolean allocateEachTime = (r % 2 == 0);
            futures.add(pool.submit(() -> readerLoop(lock, failure, allocateEachTime)));
        }

        Thread.sleep(DURATION_MS);
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        for (Future<?> f : futures) {
            f.get();
        }

        if (failure.get() != null) {
            throw new AssertionError("concurrent read observed torn snapshot", failure.get());
        }

        // 压测确实产生了读取和争用
        SeqlockStats.Snapshot stats = lock.stats().snapshot();
        assertThat(stats.readCalls()).isPositive();
        assertThat(stats.retries()).isPositive();
        assertThat(stats.retryRate()).isBetween(0.0, 1.0);
        assertThat(stats.maxRetriesPerRead()).isPositive();
        assertThat(lock.sequence() % 2).isZero();
    }

    private static void writerLoop(Seqlock<PairData> lock, AtomicReference<Throwable> failure) {
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DURATION_MS);
            while (System.nanoTime() < deadline && failure.get() == null) {
                long a = ThreadLocalRandom.current().nextLong();
                lock.write(d -> d.set(a, PairData.SUM - a));
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static void readerLoop(Seqlock<PairData> lock,
                                   AtomicReference<Throwable> failure,
                                   boolean allocateEachTime) {
        PairData reusable = new PairData();
        long reads = 0;
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DURATION_MS);
            while (System.nanoTime() < deadline && failure.get() == null) {
                PairData snapshot = allocateEachTime ? lock.read(PairData::new) : lock.read(reusable);
                if (snapshot.getA() + snapshot.getB() != PairData.SUM) {
                    throw new AssertionError("torn snapshot: a=" + snapshot.getA()
                            + ", b=" + snapshot.getB());
                }
                reads++;
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
        assertThat(reads).isPositive();
    }
}
