package com.example.payment.web;

import com.example.payment.payment.PublishException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

/** 仅转换已知业务异常，未知故障保留服务器错误语义。 */
@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<Map<String, String>> invalid(Exception error) {
        return ResponseEntity.badRequest().body(Map.of("error", "INVALID_REQUEST", "message",
                error instanceof IllegalArgumentException ? error.getMessage() : "JSON格式或字段类型不正确"));
    }
    @ExceptionHandler(PublishException.class)
    public ResponseEntity<Map<String, String>> publish(PublishException error) {
        return ResponseEntity.status(error.uncertain() ? 504 : 503)
                .body(Map.of("error", "PUBLISH_FAILED", "message", error.getMessage()));
    }
}
