package com.example.flink.aggregate;

/** Checkpoint同时保存累计值和版本；不从下游数据库回读。 */
public class ShopAggregateState {
    public long totalAmountCents;
    public long version;
    public ShopAggregateState() {
    }
    public ShopAggregateState(long totalAmountCents, long version) {
        this.totalAmountCents = totalAmountCents;
        this.version = version;
    }
}
