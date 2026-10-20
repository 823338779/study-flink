package com.example.payment.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MessageContractTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void readsSharedFixturesAndRejectsInvalidPayload() throws Exception {
        var event = mapper.readValue(Path.of(System.getProperty("contracts.dir"), "payment-event.json").toFile(),
                PaymentEventDTO.class);
        event.validate();
        assertEquals("payment-001", event.eventId());
        assertEquals(10000L, event.amountCents());
        var total = mapper.readValue(Path.of(System.getProperty("contracts.dir"), "shop-total.json").toFile(),
                ShopTotalDTO.class);
        total.validate();
        assertEquals("A:1", total.resultId());
        assertEquals(1L, total.version());
        assertThrows(IllegalArgumentException.class, () ->
                new PaymentEventDTO("p", "A", 0L, "bad").validate());
        assertThrows(IllegalArgumentException.class, () ->
                new ShopTotalDTO("A:2", "A", 100L, 1L, 1L, "p").validate());
    }
}
