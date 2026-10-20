package com.example.payment.message;

/** version 对应店铺唯一支付数，结果表示累计值而非增量。 */
public record ShopTotalDTO(String resultId, String shopId, Long totalAmountCents,
                           Long paymentCount, Long version, String triggerEventId) {
    public void validate() {
        PaymentEventDTO.requireId(shopId);
        PaymentEventDTO.requireId(triggerEventId);
        if (version == null || version <= 0 || !version.equals(paymentCount)
                || totalAmountCents == null || totalAmountCents <= 0
                || !(shopId + ":" + version).equals(resultId)) {
            throw new IllegalArgumentException("累计结果的ID、金额、计数或版本不合法");
        }
    }
}
