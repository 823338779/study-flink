package com.example.flink.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

/** Kafka使用普通JSON，不依赖Spring的Java类型消息头。 */
public final class EventJson {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
    private EventJson() {
    }
    public static PaymentEventDTO readPayment(String value) {
        try {
            var event = MAPPER.readValue(value, PaymentEventDTO.class);
            if (event == null) {
                throw new IllegalArgumentException("支付事件不能为空");
            }
            event.validate();
            return event;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("支付JSON无法解析", e);
        }
    }
    public static String writeTotal(ShopTotalDTO event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("统计JSON无法编码", e);
        }
    }
}
