package com.example.flink;

import org.apache.flink.api.java.utils.ParameterTool;

/** 独立作业参数；改变间隔时必须带状态恢复，不能重置业务版本。 */
public record JobOptions(String bootstrapServers, long checkpointIntervalMs,
                         String checkpointDir, String transactionalIdPrefix) {
    public static JobOptions parse(String[] args) {
        var parameters = ParameterTool.fromArgs(args);
        long interval = parameters.getLong("checkpoint-interval-ms", 10000);
        if (interval <= 0) {
            throw new IllegalArgumentException("Checkpoint间隔必须大于0");
        }
        return new JobOptions(parameters.get("bootstrap-servers", "kafka:9092"), interval,
                parameters.get("checkpoint-dir", "file:///opt/flink/checkpoints"),
                parameters.get("transactional-id-prefix", "payment-lab-totals-"));
    }
}
