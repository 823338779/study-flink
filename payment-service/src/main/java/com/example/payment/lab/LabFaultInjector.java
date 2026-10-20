package com.example.payment.lab;

import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 一次性故障只在lab存在，用于复现数据库提交与offset提交之间的间隙。 */
@Component
@Profile("lab")
public class LabFaultInjector {
    private static final Logger LOG = LoggerFactory.getLogger(LabFaultInjector.class);
    private final Set<String> armed = ConcurrentHashMap.newKeySet();
    private final Map<String, Map<String, Object>> observations = new ConcurrentHashMap<>();
    public void arm(String resultId) {
        if (resultId == null || !resultId.matches("[A-Za-z0-9_-]{1,64}:[1-9][0-9]{0,18}")) {
            throw new IllegalArgumentException("resultId格式应为shopId:version");
        }
        observations.remove(resultId);
        armed.add(resultId);
    }
    public void failOnceIfArmed(String resultId, String outcome, int partition, long offset) {
        if (armed.remove(resultId)) {
            observations.put(resultId, Map.of("replayed", false));
            LOG.warn("INJECT_AFTER_COMMIT resultId={}", resultId);
            throw new InjectedFailureException(resultId);
        }
        // 只记录布置过的故障；脚本再独立检查broker已提交offset，避免把回调完成当作消费确认。
        observations.computeIfPresent(resultId, (id, previous) -> Map.of(
                "replayed", true, "outcome", outcome, "partition", partition, "offset", offset));
    }
    public Map<String, Object> observation(String resultId) {
        return observations.getOrDefault(resultId, Map.of("replayed", false));
    }
    public static class InjectedFailureException extends RuntimeException {
        public InjectedFailureException(String resultId) {
            super("教学故障：数据库已提交，offset尚未提交，resultId=" + resultId);
        }
    }
}
