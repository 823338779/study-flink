package com.example.payment.stat;

/** 供看板查询的已落库累计结果。 */
public record ShopStatDTO(String shopId, long totalAmountCents, long paymentCount, long version) {
}
