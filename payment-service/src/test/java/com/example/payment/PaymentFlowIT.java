package com.example.payment;

import com.example.payment.consumer.ResultProcessingService;
import com.example.payment.consumer.ShopTotalListener;
import com.example.payment.consumer.ProcessingOutcomeEnum;
import com.example.payment.message.ShopTotalDTO;
import com.example.payment.stat.ShopStatRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.argThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("lab")
@ExtendWith(OutputCaptureExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaymentFlowIT {
    @Container static final PostgreSQLContainer PG = new PostgreSQLContainer(
            org.testcontainers.utility.DockerImageName.parse(
                    "postgres@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193")
                    .asCompatibleSubstituteFor("postgres"));
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
    @BeforeAll static void topics() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("payment-events", 2, (short) 1),
                    new NewTopic("shop-payment-totals", 2, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
    }
    @Autowired ResultProcessingService service;
    @Autowired ShopStatRepository repository;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired KafkaTemplate<String, String> template;
    @MockitoSpyBean KafkaTemplate<?, ?> templateSpy;
    @Autowired ObjectMapper mapper;
    @Autowired TestRestTemplate http;
    @MockitoSpyBean ShopTotalListener listener;

    private ShopTotalDTO total(String shop, long version, long cents) {
        return new ShopTotalDTO(shop + ":" + version, shop, cents, version, version, "p" + version);
    }
    @Test void deduplicatesAndRejectsOlderUnseenVersions() {
        assertEquals(ProcessingOutcomeEnum.APPLIED, service.process(total("db", 1, 10000)));
        assertEquals(ProcessingOutcomeEnum.DUPLICATE, service.process(total("db", 1, 10000)));
        assertEquals(ProcessingOutcomeEnum.APPLIED, service.process(total("db", 3, 23000)));
        assertEquals(ProcessingOutcomeEnum.STALE, service.process(total("db", 2, 18000)));
        assertEquals(23000, repository.find("db").orElseThrow().totalAmountCents());
        assertEquals(3, repository.find("db").orElseThrow().version());
        assertEquals(3L, jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE shop_id='db'", Long.class));
    }
    @Test void registrationAndStatsRollbackTogether() {
        assertThrows(IllegalStateException.class, () ->
                new TransactionTemplate(transactionManager).execute(status -> {
                    service.process(total("rollback", 1, 100));
                    throw new IllegalStateException("simulated failure before commit");
                }));
        assertTrue(repository.find("rollback").isEmpty());
        assertEquals(0L, jdbc.queryForObject(
                "SELECT count(*) FROM processed_event WHERE shop_id='rollback'", Long.class));
    }
    @Test void concurrentDeliveryHasOneWinner() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        try {
            var start = new CountDownLatch(1);
            Callable<ProcessingOutcomeEnum> call = () -> { start.await(); return service.process(total("race", 1, 100)); };
            var a = pool.submit(call);
            var b = pool.submit(call);
            start.countDown();
            assertEquals(Set.of(ProcessingOutcomeEnum.APPLIED, ProcessingOutcomeEnum.DUPLICATE),
                    Set.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS)));
            assertEquals(100, repository.find("race").orElseThrow().totalAmountCents());
        } finally { pool.shutdownNow(); }
    }
    @Test void validatesHttpAndPublishesRealKafkaRecord() throws Exception {
        clearInvocations(templateSpy);
        for (String body : List.of(
                "{\"eventId\":\"\",\"shopId\":\"A\",\"amountCents\":1,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":null,\"shopId\":\"A\",\"amountCents\":1,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"" + "a".repeat(65) + "\",\"shopId\":\"A\",\"amountCents\":1,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"amountCents\":-1,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"amountCents\":null,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"amountCents\":0,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"amountCents\":1.5,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p:1\",\"shopId\":\"A\",\"amountCents\":1,\"occurredAt\":\"2026-10-20T10:00:00Z\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"amountCents\":1,\"occurredAt\":\"bad\"}",
                "{\"eventId\":\"p\",\"shopId\":\"A\",\"occurredAt\":\"2026-10-20T10:00:00Z\"}")) {
            assertEquals(400, http.postForEntity("/api/payments", mapper.readTree(body), String.class)
                    .getStatusCode().value());
        }
        verifyNoInteractions(templateSpy);
        assertEquals(404, http.getForEntity("/api/shops/missing/stats", String.class).getStatusCode().value());
        var body = Map.of("eventId", "http-payment", "shopId", "http-shop", "amountCents", 10000,
                "occurredAt", "2026-10-20T10:00:00Z");
        var response = http.postForEntity("/api/payments", body, Map.class);
        assertEquals(200, response.getStatusCode().value());
        assertEquals("ACCEPTED", response.getBody().get("status"));
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            var partition = new TopicPartition("payment-events", ((Number) response.getBody().get("partition")).intValue());
            consumer.assign(List.of(partition));
            consumer.seek(partition, ((Number) response.getBody().get("offset")).longValue());
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                var records = consumer.poll(Duration.ofMillis(300));
                assertFalse(records.isEmpty());
                var record = records.iterator().next();
                assertEquals("http-shop", record.key());
                assertEquals("http-payment", mapper.readTree(record.value()).get("eventId").asText());
            });
        }
    }
    @Test void retriesAfterDatabaseCommitWithoutDuplicatingEffect(CapturedOutput output) throws Exception {
        assertEquals(200, http.postForEntity("/api/lab/fail-after-commit",
                Map.of("resultId", "lab-failure:1"), String.class).getStatusCode().value());
        var sent = template.send("shop-payment-totals", "lab-failure",
                mapper.writeValueAsString(total("lab-failure", 1, 700))).get(15, TimeUnit.SECONDS);
        var tp = new TopicPartition("shop-payment-totals", sent.getRecordMetadata().partition());
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertEquals(700, repository.find("lab-failure").orElseThrow().totalAmountCents());
            assertTrue(output.getAll().contains("INJECT_AFTER_COMMIT resultId=lab-failure:1"));
            assertTrue(output.getAll().contains("resultId=lab-failure:1 outcome=DUPLICATE"));
            var probe = http.getForEntity("/api/lab/faults/lab-failure:1", Map.class);
            assertEquals(200, probe.getStatusCode().value());
            assertEquals(true, probe.getBody().get("replayed"));
            assertEquals("DUPLICATE", probe.getBody().get("outcome"));
            assertEquals(sent.getRecordMetadata().offset(), ((Number) probe.getBody().get("offset")).longValue());
            assertEquals(tp.partition(), ((Number) probe.getBody().get("partition")).intValue());
            try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var offsets = admin.listConsumerGroupOffsets("payment-lab-projector")
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                assertTrue(offsets.get(tp).offset() > sent.getRecordMetadata().offset());
            }
        });
        assertEquals(1L, jdbc.queryForObject(
                "SELECT count(*) FROM processed_event WHERE shop_id='lab-failure'", Long.class));
    }
    @Test void invalidResultDoesNotLeaveDedupMarker() {
        assertThrows(IllegalArgumentException.class, () ->
                service.process(new ShopTotalDTO("invalid:2", "invalid", 100L, 1L, 1L, "p")));
        assertTrue(repository.find("invalid").isEmpty());
    }
    @Test void mismatchedKafkaKeyIsRejectedBeforeDatabaseMutation() throws Exception {
        var record = new ConsumerRecord<>("shop-payment-totals", 0, 0, "wrong-shop",
                mapper.writeValueAsString(total("key-check", 1, 100)));
        assertThrows(IllegalArgumentException.class, () -> listener.onMessage(record));
        assertTrue(repository.find("key-check").isEmpty());
        assertEquals(0L, jdbc.queryForObject(
                "SELECT count(*) FROM processed_event WHERE shop_id='key-check'", Long.class));
    }
    @Test
    @Order(Integer.MAX_VALUE)
    void malformedRecordDoesNotAdvancePastLaterRecord() throws Exception {
        var bad = template.send("shop-payment-totals", 0, "poison", "{").get(10, TimeUnit.SECONDS);
        template.send("shop-payment-totals", 0, "behind-poison",
                mapper.writeValueAsString(total("behind-poison", 1, 100))).get(10, TimeUnit.SECONDS);
        // 至少观察12次真实调用，超过默认有限重试9次后会跳过的边界。
        verify(listener, timeout(25000).atLeast(12)).onMessage(argThat(record ->
                record.partition() == 0 && record.offset() == bad.getRecordMetadata().offset()));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertTrue(repository.find("behind-poison").isEmpty());
            try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var offsets = admin.listConsumerGroupOffsets("payment-lab-projector")
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var committed = offsets.get(new TopicPartition("shop-payment-totals", 0));
                assertTrue(committed == null || committed.offset() <= bad.getRecordMetadata().offset());
            }
        });
    }
}
