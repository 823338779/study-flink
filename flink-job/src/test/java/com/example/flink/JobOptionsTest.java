package com.example.flink;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JobOptionsTest {
    @Test void rejectsNonpositiveInterval() {
        assertThrows(IllegalArgumentException.class, () ->
                JobOptions.parse(new String[]{"--checkpoint-interval-ms", "0"}));
    }
    @Test void acceptsRecoveryCompatibleArguments() {
        var options = JobOptions.parse(new String[]{"--checkpoint-interval-ms", "30000"});
        assertEquals(30000, options.checkpointIntervalMs());
        assertEquals("kafka:9092", options.bootstrapServers());
    }
}
