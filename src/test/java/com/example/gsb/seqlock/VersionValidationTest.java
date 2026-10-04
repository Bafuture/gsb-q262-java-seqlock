package com.example.gsb.seqlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** 版本号协议：写者进入 +1（奇）、写完 +1（偶），读者读前后校验一致。 */
class VersionValidationTest {

    @Test
    void initialVersionIsZeroAndEven() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);

        assertThat(lock.sequence()).isZero();
        assertThat(lock.sequence() % 2).isZero();
        assertThat(lock.writeInProgress()).isFalse();
    }

    @Test
    void eachWriteBumpsVersionByTwoAndStaysEven() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);

        for (int i = 1; i <= 5; i++) {
            int expected = 2 * i;
            long value = i;
            lock.write(d -> d.set(value, PairData.SUM - value));

            assertThat(lock.sequence()).isEqualTo(expected);
            assertThat(lock.sequence() % 2).isZero();
            assertThat(lock.writeInProgress()).isFalse();
        }
    }

    @Test
    void versionIsOddWhileWriteInProgress() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        AtomicInteger observedInsideWrite = new AtomicInteger();
        AtomicBoolean inProgressInsideWrite = new AtomicBoolean();

        lock.write(d -> {
            observedInsideWrite.set(lock.sequence());
            inProgressInsideWrite.set(lock.writeInProgress());
            d.set(1, PairData.SUM - 1);
        });

        assertThat(observedInsideWrite.get()).isOdd();
        assertThat(inProgressInsideWrite).isTrue();
        assertThat(lock.sequence()).isEven();
    }

    @Test
    void readDoesNotChangeVersion() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        lock.write(d -> d.set(3, PairData.SUM - 3));
        int version = lock.sequence();

        lock.read(new PairData());
        lock.read(PairData::new);

        assertThat(lock.sequence()).isEqualTo(version);
    }

    @Test
    void readReturnsDataOfLatestCommittedVersion() {
        Seqlock<PairData> lock = new Seqlock<>(new PairData(), PairData::copyTo);
        lock.write(d -> d.set(11, PairData.SUM - 11));
        lock.write(d -> d.set(22, PairData.SUM - 22));

        PairData snapshot = lock.read(new PairData());

        assertThat(snapshot.getA()).isEqualTo(22);
        assertThat(snapshot.getB()).isEqualTo(PairData.SUM - 22);
        assertThat(snapshot.satisfiesInvariant()).isTrue();
    }
}
