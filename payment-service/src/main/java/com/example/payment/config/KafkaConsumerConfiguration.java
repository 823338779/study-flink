package com.example.payment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import java.util.Map;

/** 教学环境不静默跳过错误记录：失败则重试，阻止同分区进度越过它。 */
@Configuration
public class KafkaConsumerConfiguration {
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
}
