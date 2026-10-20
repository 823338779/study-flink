package com.example.payment.payment;

/** Kafka 已确认接收的位置，不表示业务统计已完成。 */
public record PublishReceiptDTO(String eventId, String status, String topic, int partition, long offset) {
}
