package com.example.gsb.seqlock;

import java.util.Objects;

/**
 * 示例复合数据：两个字段 {@code a}、{@code b} 必须满足不变式 {@code a + b == SUM}。
 *
 * <p>该不变式用于并发压测：任何撕裂/跨版本的快照都会让 {@code a + b} 不等于 {@link #SUM}。</p>
 */
public final class PairData {

    /** a + b 的恒定值。 */
    public static final long SUM = 1_000_000L;

    private long a;
    private long b;

    public PairData() {
        this(0L, SUM);
    }

    public PairData(long a, long b) {
        this.a = a;
        this.b = b;
    }

    public long getA() {
        return a;
    }

    public long getB() {
        return b;
    }

    /** 原子地（在写者临界区内）同时设置两个字段并维持不变式。 */
    public void set(long newA, long newB) {
        if (newA + newB != SUM) {
            throw new IllegalArgumentException("invariant violated: a + b must equal " + SUM);
        }
        this.a = newA;
        this.b = newB;
    }

    public void setA(long newA) {
        this.a = newA;
    }

    public void setB(long newB) {
        this.b = newB;
    }

    /** 把当前快照完整复制到目标实例（供 Seqlock 的 copier 使用）。 */
    public void copyTo(PairData target) {
        target.a = this.a;
        target.b = this.b;
    }

    /** 快照是否满足字段间约束关系。 */
    public boolean satisfiesInvariant() {
        return a + b == SUM;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PairData other)) {
            return false;
        }
        return a == other.a && b == other.b;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }

    @Override
    public String toString() {
        return "PairData{a=" + a + ", b=" + b + ", sum=" + (a + b) + "}";
    }
}
