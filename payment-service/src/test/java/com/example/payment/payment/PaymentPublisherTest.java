package com.example.payment.payment;

import com.example.payment.message.PaymentEventDTO;
import com.example.payment.web.ApiExceptionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PaymentPublisherTest {
    @SuppressWarnings("unchecked")
    @Test
    void returnsReceiptOnlyAfterKafkaAcknowledgmentAndKeepsStableKey() throws Exception {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        var pending = new CompletableFuture<SendResult<String, String>>();
        when(template.send(eq("payment-events"), eq("A"), anyString())).thenReturn(pending);
        var publisher = new PaymentPublisher(template, new ObjectMapper(), Duration.ofMillis(100));
        var event = new PaymentEventDTO("p1", "A", 20L, "2026-10-20T10:00:00Z");
        var timeout = assertThrows(PublishException.class, () -> publisher.publish(event));
        assertTrue(timeout.uncertain());
        assertEquals(504, new ApiExceptionHandler().publish(timeout).getStatusCode().value());
        pending.complete(new SendResult<>(null,
                new RecordMetadata(new TopicPartition("payment-events", 1), 7, 0, 0, 0, 0)));
        var receipt = publisher.publish(event);
        assertEquals(7, receipt.offset());
        assertEquals(1, receipt.partition());
        assertEquals("p1", receipt.eventId());
        assertEquals("ACCEPTED", receipt.status());
    }
    @SuppressWarnings("unchecked")
    @Test
    void producerFailureIsNotReportedAsSuccess() {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("offline")));
        var publisher = new PaymentPublisher(template, new ObjectMapper(), Duration.ofMillis(100));
        var error = assertThrows(PublishException.class, () ->
                publisher.publish(new PaymentEventDTO("p1", "A", 20L, "2026-10-20T10:00:00Z")));
        assertFalse(error.uncertain());
        assertEquals(503, new ApiExceptionHandler().publish(error).getStatusCode().value());
    }
}
