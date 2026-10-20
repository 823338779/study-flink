package com.example.flink.aggregate;

import com.example.flink.message.PaymentEventDTO;
import com.example.flink.message.ShopTotalDTO;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PaymentAggregateFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, PaymentEventDTO, ShopTotalDTO> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(
                new PaymentAggregateFunction()), e -> e.shopId, Types.STRING);
    }
    private PaymentEventDTO event(String id, String shop, long cents) {
        return new PaymentEventDTO(id, shop, cents, "2026-10-20T10:00:00Z");
    }
    private List<ShopTotalDTO> results(KeyedOneInputStreamOperatorTestHarness<String, PaymentEventDTO, ShopTotalDTO> h) {
        return h.extractOutputValues();
    }
    @Test
    void aggregatesPerShopAndDeduplicates() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("p1", "A", 10000)));
            h.processElement(new StreamRecord<>(event("p1", "B", 5000)));
            h.processElement(new StreamRecord<>(event("p2", "A", 8000)));
            h.processElement(new StreamRecord<>(event("p1", "A", 10000)));
            assertEquals(3, results(h).size());
            assertEquals(18000, results(h).get(2).totalAmountCents);
            assertEquals("A:2", results(h).get(2).resultId);
            assertEquals(5000, results(h).get(1).totalAmountCents);
        }
    }
    @Test
    void restoresStateAndReplaysWithStableResultIdentity() throws Exception {
        var first = harness();
        first.open();
        first.processElement(new StreamRecord<>(event("p1", "A", 18000)));
        var snapshot = first.snapshot(1, 1);
        first.processElement(new StreamRecord<>(event("p2", "A", 2000)));
        first.processElement(new StreamRecord<>(event("p3", "A", 3000)));
        String expectedId = results(first).get(2).resultId;
        first.close();
        try (var restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(new StreamRecord<>(event("p1", "A", 18000)));
            restored.processElement(new StreamRecord<>(event("p2", "A", 2000)));
            restored.processElement(new StreamRecord<>(event("p3", "A", 3000)));
            assertEquals(2, results(restored).size());
            assertEquals(23000, results(restored).get(1).totalAmountCents);
            assertEquals(expectedId, results(restored).get(1).resultId);
        }
    }
    @Test
    void conflictingPayloadAndOverflowDoNotProduceResults() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("p1", "A", Long.MAX_VALUE)));
            assertThrows(IllegalArgumentException.class, () ->
                    h.processElement(new StreamRecord<>(event("p1", "A", 1))));
            var changed = event("p1", "A", Long.MAX_VALUE);
            changed.occurredAt = "2026-10-20T11:00:00Z";
            assertThrows(IllegalArgumentException.class, () -> h.processElement(new StreamRecord<>(changed)));
            assertThrows(ArithmeticException.class, () ->
                    h.processElement(new StreamRecord<>(event("p2", "A", 1))));
            h.processElement(new StreamRecord<>(event("p1", "A", Long.MAX_VALUE)));
            assertEquals(1, results(h).size());
        }
    }
}
