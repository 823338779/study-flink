package com.example.flink;

import com.example.flink.aggregate.PaymentAggregateFunction;
import com.example.flink.message.EventJson;
import com.example.flink.message.ShopTotalDTO;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * 第一课无窗口累计，后续可在此基础上引入事件时间和窗口。
 */
public final class PaymentAggregationJob {
    public static void main(String[] args) throws Exception {
        var options = JobOptions.parse(args);
        var env = createEnvironment(options);
        env.setParallelism(2);
        env.enableCheckpointing(options.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage(options.checkpointDir());
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(3)));
        var source = KafkaSource.<String>builder()
                .setBootstrapServers(options.bootstrapServers())
                .setTopics("payment-events").setGroupId("payment-lab-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var sink = KafkaSink.<ShopTotalDTO>builder()
                .setBootstrapServers(options.bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(options.transactionalIdPrefix())
                .setProperty("transaction.timeout.ms", "600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<ShopTotalDTO>builder()
                        .setTopic("shop-payment-totals")
                        .setKeySerializationSchema(e -> e.shopId.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(e -> EventJson.writeTotal(e).getBytes(StandardCharsets.UTF_8))
                        .build()).build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "payment-events").uid("payment-source")
                .map(EventJson::readPayment).name("parse-payment").uid("parse-payment")
                .keyBy(e -> e.shopId)
                .process(new PaymentAggregateFunction()).name("shop-aggregate").uid("shop-aggregate")
                .sinkTo(sink).name("totals-sink").uid("totals-sink");
        env.execute("payment-aggregation");
    }

    /** 本地调试显式开启独立Dashboard；集群提交沿用客户端提供的执行环境。 */
    static StreamExecutionEnvironment createEnvironment(JobOptions options) {
        if (!options.localWebUi()) {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
        var config = new Configuration();
        config.set(RestOptions.PORT, 18082);
        config.set(RestOptions.ADDRESS, "127.0.0.1");
        config.set(RestOptions.BIND_ADDRESS, "127.0.0.1");
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    }
}
