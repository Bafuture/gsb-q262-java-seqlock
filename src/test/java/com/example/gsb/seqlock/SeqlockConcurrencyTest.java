package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 并发压测：多写者持续搬移、多读者持续读快照，
 * 每次读到的 a、b 都必须来自同一版本，即 a + b 恒定，绝不允许撕裂。
 */
class SeqlockConcurrencyTest {

    @Test
    @Timeout(120)
    void snapshotsNeverTearUnderConcurrentWritersAndReaders() throws Exception {
        int writers = 4;
        int readers = 4;
        int writesPerWriter = 25_000;

        ConsistentPair pair = new ConsistentPair(1_000_000L, 1_000_000L);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch writerDone = new CountDownLatch(writers);
        AtomicLong successfulReads = new AtomicLong();
        AtomicReference<ConsistentPair.Snapshot> firstTorn = new AtomicReference<>();

        List<Thread> threads = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
            Thread t = new Thread(() -> {
                await(start);
                for (int i = 0; i < writesPerWriter; i++) {
                    long delta = ThreadLocalRandom.current().nextLong(-100, 101);
                    pair.transfer(delta);
                }
                writerDone.countDown();
            }, "writer-" + w);
            t.setDaemon(true);
            threads.add(t);
        }

        for (int r = 0; r < readers; r++) {
            Thread t = new Thread(() -> {
                await(start);
                while (writerDone.getCount() > 0) {
                    ConsistentPair.Snapshot s = pair.snapshot();
                    successfulReads.incrementAndGet();
                    if (s.sum() != pair.invariantSum()) {
                        firstTorn.compareAndSet(null, s);
                    }
                }
            }, "reader-" + r);
            t.setDaemon(true);
            threads.add(t);
        }

        threads.forEach(Thread::start);
        start.countDown();

        assertThat(writerDone.await(60, TimeUnit.SECONDS)).isTrue();
        for (Thread t : threads) {
            t.join(10_000);
        }
        for (Thread t : threads) {
            assertThat(t.isAlive()).as(() -> "thread still alive: " + t.getName()).isFalse();
        }

        // 核心正确性断言：并发过程中数十万次读取，没有任何一次读到撕裂值
        assertThat(firstTorn.get())
                .as("读到撕裂快照（a、b 来自不同版本）")
                .isNull();
        assertThat(successfulReads.get()).isGreaterThan(0L);

        ConsistentPair.Snapshot finalSnapshot = pair.snapshot();
        assertThat(finalSnapshot.sum()).isEqualTo(pair.invariantSum());

        SeqlockStats stats = pair.seqlock().stats();
        assertThat(stats.reads()).isEqualTo(successfulReads.get() + 1L);
        assertThat(stats.retries()).isGreaterThanOrEqualTo(0L);
        assertThat(stats.retryRate()).isGreaterThanOrEqualTo(0.0d);
        // 写入都很短，绝大多数读靠自旋完成，降级加锁只是极少数兜底
        assertThat(stats.fallbacks()).isLessThanOrEqualTo(Math.max(1L, stats.reads() / 100));
    }

    @Test
    @Timeout(60)
    void writesFromMultipleThreadsAreSerialized() throws Exception {
        int threads = 8;
        int incrementsPerThread = 10_000;
        Seqlock lock = new Seqlock();
        long[] counter = {0L};
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                await(start);
                for (int j = 0; j < incrementsPerThread; j++) {
                    lock.write(() -> counter[0]++);
                }
                done.countDown();
            });
            t.setDaemon(true);
            workers.add(t);
        }
        workers.forEach(Thread::start);
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(counter[0]).isEqualTo((long) threads * incrementsPerThread);
        assertThat(lock.version()).isEqualTo(2L * threads * incrementsPerThread);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
