package com.example.payment.payment;

import com.example.payment.message.PaymentEventDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 等待broker确认后返回；不在HTTP请求里双写数据库。 */
@Service
public class PaymentPublisher {
    private final KafkaTemplate<String, String> template;
    private final ObjectMapper mapper;
    private final Duration timeout;

    @Autowired
    public PaymentPublisher(KafkaTemplate<String, String> template, ObjectMapper mapper) {
        this(template, mapper, Duration.ofSeconds(10));
    }

    public PaymentPublisher(KafkaTemplate<String, String> template, ObjectMapper mapper, Duration timeout) {
        this.template = template;
        this.mapper = mapper;
        this.timeout = timeout;
    }

    public PublishReceiptDTO publish(PaymentEventDTO event) {
        event.validate();
        var normalized = new PaymentEventDTO(event.eventId(), event.shopId(), event.amountCents(),
                Instant.parse(event.occurredAt()).toString());
        try {
            var metadata = template.send("payment-events", event.shopId(), mapper.writeValueAsString(normalized))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getRecordMetadata();
            return new PublishReceiptDTO(event.eventId(), "ACCEPTED", metadata.topic(),
                    metadata.partition(), metadata.offset());
        } catch (TimeoutException e) {
            throw new PublishException(true, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException(true, e);
        } catch (Exception e) {
            throw new PublishException(false, e);
        }
    }
}
