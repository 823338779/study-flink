package com.example.flink.message;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** 显式POJO用于Flink状态序列化，金额单位为分。 */
public class PaymentEventDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    public String eventId;
    public String shopId;
    public Long amountCents;
    public String occurredAt;

    public PaymentEventDTO() {
    }
    public PaymentEventDTO(String eventId, String shopId, Long amountCents, String occurredAt) {
        this.eventId = eventId;
        this.shopId = shopId;
        this.amountCents = amountCents;
        this.occurredAt = occurredAt;
    }
    public void validate() {
        if (eventId == null || !eventId.matches("[A-Za-z0-9_-]{1,64}")
                || shopId == null || !shopId.matches("[A-Za-z0-9_-]{1,64}")
                || amountCents == null || amountCents <= 0) {
            throw new IllegalArgumentException("支付事件ID或金额不合法");
        }
        try {
            occurredAt = Instant.parse(occurredAt).toString();
        } catch (Exception e) {
            throw new IllegalArgumentException("occurredAt不是ISO时间", e);
        }
    }
    public boolean samePayload(PaymentEventDTO other) {
        return Objects.equals(shopId, other.shopId) && Objects.equals(eventId, other.eventId)
                && Objects.equals(amountCents, other.amountCents) && Objects.equals(occurredAt, other.occurredAt);
    }
}
