package com.example.payment.stat;

import com.example.payment.message.PaymentEventDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

/** 查询消费端已经提交到PostgreSQL的结果。 */
@RestController
public class ShopStatController {
    private final ShopStatRepository repository;
    public ShopStatController(ShopStatRepository repository) {
        this.repository = repository;
    }
    @GetMapping("/api/shops/{shopId}/stats")
    public ResponseEntity<ShopStatDTO> find(@PathVariable String shopId) {
        PaymentEventDTO.requireId(shopId);
        return ResponseEntity.of(repository.find(shopId));
    }
    @GetMapping("/api/processed-events/count")
    public Map<String, Long> count() {
        return Map.of("count", repository.countProcessed());
    }
}
