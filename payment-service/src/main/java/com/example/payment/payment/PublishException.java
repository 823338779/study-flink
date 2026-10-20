package com.example.payment.payment;

/** 超时无法判断Kafka最终是否接收，调用方必须使用原事件重试。 */
public class PublishException extends RuntimeException {
    private final boolean uncertain;

    public PublishException(boolean uncertain, Throwable cause) {
        super(uncertain ? "发送结果不确定，请使用原事件ID和载荷重试" : "Kafka发送失败，请使用原事件重试", cause);
        this.uncertain = uncertain;
    }

    public boolean uncertain() {
        return uncertain;
    }
}
