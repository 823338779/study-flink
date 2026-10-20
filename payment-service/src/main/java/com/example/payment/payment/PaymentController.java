package com.example.payment.payment;

import com.example.payment.message.PaymentEventDTO;
import org.springframework.web.bind.annotation.*;

/** 接收模拟支付事件。 */
@RestController
@RequestMapping("/api/payments")
public class PaymentController {
    private final PaymentPublisher publisher;
    public PaymentController(PaymentPublisher publisher) {
        this.publisher = publisher;
    }
    @PostMapping
    public PublishReceiptDTO publish(@RequestBody PaymentEventDTO event) {
        return publisher.publish(event);
    }
}
