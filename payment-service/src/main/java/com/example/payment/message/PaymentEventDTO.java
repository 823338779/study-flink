package com.example.payment.message;

import java.time.Instant;

/** 金额单位为分；重试必须保留事件身份和载荷。 */
public record PaymentEventDTO(String eventId, String shopId, Long amountCents, String occurredAt) {
    public void validate() {
        requireId(eventId);
        requireId(shopId);
        if (amountCents == null || amountCents <= 0) {
            throw new IllegalArgumentException("amountCents 必须是正整数（分）");
        }
        try {
            Instant.parse(occurredAt);
        } catch (Exception e) {
            throw new IllegalArgumentException("occurredAt 必须是 ISO-8601 时间", e);
        }
    }

    public static void requireId(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("ID 必须是1至64位字母、数字、下划线或短横线");
        }
    }
}
