package com.example.flink.message;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MessageContractTest {
    @Test
    void readsSharedPaymentAndWritesSharedTotal() throws Exception {
        String json = Files.readString(Path.of(System.getProperty("contracts.dir"), "payment-event.json"));
        var event = EventJson.readPayment(json);
        assertEquals("A", event.shopId);
        assertEquals(10000L, event.amountCents);
        var total = new ShopTotalDTO("A:1", "A", 10000L, 1L, 1L, "payment-001");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        assertEquals(mapper.readTree(Files.readString(Path.of(System.getProperty("contracts.dir"),
                "shop-total.json"))), mapper.readTree(EventJson.writeTotal(total)));
        assertThrows(Exception.class, () -> EventJson.readPayment("{\"shopId\":\"A\"}"));
        assertThrows(Exception.class, () -> EventJson.readPayment(json.replace("2026-10-20T10:00:00Z", "bad")));
    }
}
