package com.example.flink;

import org.apache.flink.api.java.utils.ParameterTool;

/** 独立作业参数；改变间隔时必须带状态恢复，不能重置业务版本。 */
public record JobOptions(String bootstrapServers, long checkpointIntervalMs,
                         String checkpointDir, String transactionalIdPrefix, boolean localWebUi) {
    public static JobOptions parse(String[] args) {
        var parameters = ParameterTool.fromArgs(args);
        long interval = parameters.getLong("checkpoint-interval-ms", 10000);
        if (interval <= 0) {
            throw new IllegalArgumentException("Checkpoint间隔必须大于0");
        }
        boolean localWebUi = parameters.has("local-web-ui");
        // 容器服务名仅在Compose网络内可解析，本机调试通过发布端口访问Kafka。
        String defaultBootstrap = localWebUi ? "localhost:19092" : "kafka:9092";
        return new JobOptions(parameters.get("bootstrap-servers", defaultBootstrap), interval,
                parameters.get("checkpoint-dir", "file:///opt/flink/checkpoints"),
                parameters.get("transactional-id-prefix", "payment-lab-totals-"),
                localWebUi);
    }
}
