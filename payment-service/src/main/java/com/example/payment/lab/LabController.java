package com.example.payment.lab;

import com.example.payment.message.ShopTotalDTO;
import com.example.payment.payment.PublishException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/** 仅用于本地实验，历史结果注入限定lab-店铺。 */
@RestController
@Profile("lab")
@RequestMapping("/api/lab")
public class LabController {
    private final LabFaultInjector faults;
    private final KafkaTemplate<String, String> template;
    private final ObjectMapper mapper;
    public LabController(LabFaultInjector faults, KafkaTemplate<String, String> template, ObjectMapper mapper) {
        this.faults = faults;
        this.template = template;
        this.mapper = mapper;
    }
    @PostMapping("/fail-after-commit")
    public Map<String, String> arm(@RequestBody Map<String, String> request) {
        faults.arm(request.get("resultId"));
        return Map.of("armed", request.get("resultId"));
    }
    @GetMapping("/faults/{resultId}")
    public Map<String, Object> observation(@PathVariable String resultId) {
        return faults.observation(resultId);
    }
    @PostMapping("/results")
    public Map<String, Object> inject(@RequestBody ShopTotalDTO event) {
        event.validate();
        if (!event.shopId().startsWith("lab-")) {
            throw new IllegalArgumentException("结果注入仅允许lab-开头的店铺");
        }
        try {
            var metadata = template.send("shop-payment-totals", event.shopId(), mapper.writeValueAsString(event))
                    .get(10, TimeUnit.SECONDS).getRecordMetadata();
            return Map.of("resultId", event.resultId(), "partition", metadata.partition(), "offset", metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException(true, e);
        } catch (Exception e) {
            throw new PublishException(true, e);
        }
    }
}
