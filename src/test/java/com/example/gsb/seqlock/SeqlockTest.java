package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SeqlockTest {

    // ---------- 版本号校验 ----------

    @Test
    void versionStartsAtZeroAndIsEven() {
        Seqlock lock = new Seqlock();
        assertThat(lock.version()).isZero();
        assertThat(lock.version() % 2).isZero();
    }

    @Test
    void writeIncrementsVersionByTwoAndEndsEven() {
        Seqlock lock = new Seqlock();
        lock.write(() -> {});
        assertThat(lock.version()).isEqualTo(2L);
        lock.write(() -> {});
        assertThat(lock.version()).isEqualTo(4L);
        assertThat(lock.version() % 2).isZero();
    }

    @Test
    @Timeout(10)
    void versionIsOddWhileWriteInProgress() throws Exception {
        Seqlock lock = new Seqlock();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread writer = new Thread(() -> lock.write(() -> {
            entered.countDown();
            await(release);
        }));
        writer.start();

        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lock.version()).isOdd(); // 写者已进入：版本为奇

        release.countDown();
        writer.join(5_000);
        assertThat(writer.isAlive()).isFalse();
        assertThat(lock.version()).isEqualTo(2L); // 写完：回到偶数
    }

    @Test
    void writerExceptionStillRestoresEvenVersion() {
        Seqlock lock = new Seqlock();
        assertThatThrownBy(() -> lock.write(() -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(lock.version()).isEqualTo(2L); // 不会卡在奇数
        // 后续读写不受影响
        lock.write(() -> {});
        assertThat(lock.read(() -> 42)).isEqualTo(42);
        assertThat(lock.version()).isEqualTo(4L);
    }

    // ---------- 复合结构字段一致性 ----------

    @Test
    void readReturnsConsistentSnapshotOfCompoundFields() {
        ConsistentPair pair = new ConsistentPair(100, 200);
        pair.transfer(30);
        pair.transfer(-50);

        ConsistentPair.Snapshot s = pair.snapshot();
        assertThat(s.a()).isEqualTo(80);
        assertThat(s.b()).isEqualTo(220);
        assertThat(s.sum()).isEqualTo(pair.invariantSum());
    }

    // ---------- 重试上限与超限处理 ----------

    @Test
    @Timeout(10)
    void tryReadThrowsAfterMaxRetriesWhenWriteInProgress() throws Exception {
        Seqlock lock = new Seqlock(5);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread writer = new Thread(() -> lock.write(() -> {
            entered.countDown();
            await(release);
        }));
        writer.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        // 版本一直是奇数，读者必然重试到上限后抛异常
        assertThatThrownBy(() -> lock.tryRead(() -> 1))
                .isInstanceOf(RetryLimitExceededException.class)
                .hasMessageContaining("5");

        SeqlockStats stats = lock.stats();
        assertThat(stats.reads()).isZero();          // 没有成功读取
        assertThat(stats.retries()).isEqualTo(5);    // 重试次数 == 上限
        assertThat(stats.maxRetries()).isEqualTo(5); // 单次最大重试 == 上限

        release.countDown();
        writer.join(5_000);
    }

    @Test
    @Timeout(10)
    void readFallsBackToLockedReadAfterMaxRetries() throws Exception {
        Seqlock lock = new Seqlock(5);
        ConsistentPair pair = new ConsistentPair(7, 3, lock);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // 写者长时间持有（模拟慢写）
        Thread writer = new Thread(() -> lock.write(() -> {
            entered.countDown();
            await(release);
        }));
        writer.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        FutureTask<ConsistentPair.Snapshot> readTask =
                new FutureTask<>(pair::snapshot);
        Thread reader = new Thread(readTask);
        reader.start();

        // 给读者足够时间自旋到上限并阻塞在写锁上
        Thread.sleep(200);
        assertThat(reader.isAlive()).isTrue();

        release.countDown(); // 写者完成后读者才能拿到锁继续
        ConsistentPair.Snapshot s = readTask.get(5, TimeUnit.SECONDS);
        reader.join(5_000);

        assertThat(s.sum()).isEqualTo(10L);
        assertThat(lock.stats().fallbacks()).isEqualTo(1); // 确实走了降级路径
        assertThat(lock.stats().reads()).isEqualTo(1);
    }

    // ---------- 写者优先：读者不阻塞写者 ----------

    @Test
    @Timeout(10)
    void writersAreNeverBlockedByReaders() throws Exception {
        Seqlock lock = new Seqlock();
        CountDownLatch readerInside = new CountDownLatch(1);
        CountDownLatch letReaderFinish = new CountDownLatch(1);
        AtomicBoolean writerDone = new AtomicBoolean(false);

        // 读者在读取回调里长时间停留（模拟慢读者）
        Thread reader = new Thread(() -> lock.read(() -> {
            readerInside.countDown();
            await(letReaderFinish);
            return 1;
        }));
        reader.start();
        assertThat(readerInside.await(5, TimeUnit.SECONDS)).isTrue();

        // 读者还"卡在读的过程中"，写者无需等待即可完成写入
        lock.write(() -> writerDone.set(true));
        assertThat(writerDone).isTrue();

        letReaderFinish.countDown();
        reader.join(5_000);
        // 读者第一次校验失败（版本变了），重试后仍成功完成
        assertThat(lock.stats().reads()).isEqualTo(1);
        assertThat(lock.stats().retries()).isGreaterThanOrEqualTo(1);
    }

    // ---------- 统计 ----------

    @Test
    void statsTrackReadsRetriesAndRate() {
        Seqlock lock = new Seqlock(3);
        for (int i = 0; i < 4; i++) {
            int expected = i;
            assertThat(lock.read(() -> expected)).isEqualTo(i);
        }
        SeqlockStats stats = lock.stats();
        assertThat(stats.reads()).isEqualTo(4);
        assertThat(stats.retries()).isZero();
        assertThat(stats.retryRate()).isZero();
        assertThat(stats.maxRetries()).isZero();
        assertThat(stats.fallbacks()).isZero();
    }

    @Test
    @Timeout(10)
    void statsTrackRetryRateAcrossSuccessAndFailure() throws Exception {
        Seqlock lock = new Seqlock(4);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread writer = new Thread(() -> lock.write(() -> {
            entered.countDown();
            await(release);
        }));
        writer.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> lock.tryRead(() -> 1))
                .isInstanceOf(RetryLimitExceededException.class); // 记 4 次重试，无成功读

        release.countDown();
        writer.join(5_000);

        assertThat(lock.read(() -> "ok")).isEqualTo("ok"); // 1 次成功读，0 重试

        SeqlockStats stats = lock.stats();
        assertThat(stats.reads()).isEqualTo(1);
        assertThat(stats.retries()).isEqualTo(4);
        assertThat(stats.retryRate()).isEqualTo(4.0d);
        assertThat(stats.maxRetries()).isEqualTo(4);
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
