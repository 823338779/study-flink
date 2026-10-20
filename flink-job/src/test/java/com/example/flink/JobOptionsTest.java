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
        assertFalse(options.localWebUi());
    }
    @Test void enablesLocalDashboardOnlyWhenRequested() {
        var options = JobOptions.parse(new String[]{"--local-web-ui"});
        assertTrue(options.localWebUi());
        assertEquals("localhost:19092", options.bootstrapServers());
    }
    @Test void explicitKafkaAddressOverridesLocalDefault() {
        var options = JobOptions.parse(new String[]{
                "--local-web-ui", "--bootstrap-servers", "other-broker:9092"});
        assertEquals("other-broker:9092", options.bootstrapServers());
    }
}
