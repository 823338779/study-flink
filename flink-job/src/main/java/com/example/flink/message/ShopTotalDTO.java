package com.example.flink.message;

import java.io.Serializable;

/** 累计结果；version在连续状态历史内稳定递增。 */
public class ShopTotalDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    public String resultId;
    public String shopId;
    public Long totalAmountCents;
    public Long paymentCount;
    public Long version;
    public String triggerEventId;

    public ShopTotalDTO() {
    }
    public ShopTotalDTO(String resultId, String shopId, Long totalAmountCents, Long paymentCount,
                        Long version, String triggerEventId) {
        this.resultId = resultId;
        this.shopId = shopId;
        this.totalAmountCents = totalAmountCents;
        this.paymentCount = paymentCount;
        this.version = version;
        this.triggerEventId = triggerEventId;
    }
}
