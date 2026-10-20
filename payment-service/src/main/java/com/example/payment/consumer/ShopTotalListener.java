package com.example.payment.consumer;

import com.example.payment.lab.LabFaultInjector;
import com.example.payment.message.ShopTotalDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 数据库提交成功之后才返回，由容器提交offset；不在这里吞掉异常。 */
@Component
public class ShopTotalListener {
    private static final Logger LOG = LoggerFactory.getLogger(ShopTotalListener.class);
    private final ResultProcessingService service;
    private final ObjectMapper mapper;
    private final ObjectProvider<LabFaultInjector> faults;
    public ShopTotalListener(ResultProcessingService service, ObjectMapper mapper,
                             ObjectProvider<LabFaultInjector> faults) {
        this.service = service;
        this.mapper = mapper;
        this.faults = faults;
    }
    @KafkaListener(topics = "shop-payment-totals")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        var event = mapper.readValue(record.value(), ShopTotalDTO.class);
        event.validate();
        if (!event.shopId().equals(record.key())) {
            throw new IllegalArgumentException("Kafka key与shopId不一致");
        }
        var outcome = service.process(event);
        LOG.info("DATABASE_COMMITTED resultId={} outcome={} partition={} offset={}",
                event.resultId(), outcome, record.partition(), record.offset());
        var fault = faults.getIfAvailable();
        if (fault != null) {
            fault.failOnceIfArmed(event.resultId(), outcome.name(), record.partition(), record.offset());
        }
    }
}
