package com.example.gsb.seqlock;

/**
 * 复合结构示例：两个字段 {@code a}、{@code b} 必须作为整体被一致地读写，
 * 不变量为 {@code a + b == sum}（构造时确定，恒定不变）。
 *
 * <p>字段是普通 long，不加锁、不加 volatile：写者通过 {@link Seqlock#write}
 * 的 release 语义发布，读者通过 {@link Seqlock#read} 的 acquire 语义订阅，
 * 由版本号协议保证一次读取拿到的两个字段来自同一版本，不会撕裂。
 */
public final class ConsistentPair {

    /** 一致快照。 */
    public record Snapshot(long a, long b) {
        public long sum() {
            return a + b;
        }
    }

    private final Seqlock seqlock;
    private final long sum;

    private long a;
    private long b;

    public ConsistentPair(long a, long b) {
        this(a, b, new Seqlock());
    }

    public ConsistentPair(long a, long b, Seqlock seqlock) {
        this.seqlock = seqlock;
        this.a = a;
        this.b = b;
        this.sum = a + b;
    }

    /** 写：从 a 搬 delta 到 b，保持 a + b 不变。 */
    public void transfer(long delta) {
        seqlock.write(() -> {
            a += delta;
            b -= delta;
        });
    }

    /** 读：整体一致的快照，a、b 保证来自同一版本。 */
    public Snapshot snapshot() {
        return seqlock.read(() -> new Snapshot(a, b));
    }

    /** 不变量要求的恒定和。 */
    public long invariantSum() {
        return sum;
    }

    public Seqlock seqlock() {
        return seqlock;
    }
}
