package com.example.flink.aggregate;

import com.example.flink.message.PaymentEventDTO;
import com.example.flink.message.ShopTotalDTO;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** keyBy之后每家店铺独立记账；相同业务事件只产生一次累计更新。 */
public class PaymentAggregateFunction extends KeyedProcessFunction<String, PaymentEventDTO, ShopTotalDTO> {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentAggregateFunction.class);
    private transient MapState<String, PaymentEventDTO> seen;
    private transient ValueState<ShopAggregateState> aggregate;

    @Override
    public void open(OpenContext context) throws Exception {
        seen = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "seen-payments", Types.STRING, Types.POJO(PaymentEventDTO.class)));
        aggregate = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "shop-totals", Types.POJO(ShopAggregateState.class)));
    }
    @Override
    public void processElement(PaymentEventDTO event, Context context, Collector<ShopTotalDTO> out) throws Exception {
        event.validate();
        var previous = seen.get(event.eventId);
        if (previous != null) {
            if (!previous.samePayload(event)) {
                throw new IllegalArgumentException("相同事件ID载荷冲突: " + event.shopId + "/" + event.eventId);
            }
            LOG.info("PAYMENT_DUPLICATE shopId={} eventId={}", event.shopId, event.eventId);
            return;
        }
        var old = aggregate.value();
        long amount = Math.addExact(old == null ? 0 : old.totalAmountCents, event.amountCents);
        long version = Math.addExact(old == null ? 0 : old.version, 1L);
        // 校验和溢出检查成功后才写状态，输出身份与恢复后的输入历史绑定。
        seen.put(event.eventId, event);
        aggregate.update(new ShopAggregateState(amount, version));
        var result = new ShopTotalDTO(event.shopId + ":" + version, event.shopId, amount,
                version, version, event.eventId);
        LOG.info("CALCULATED resultId={} totalAmountCents={} triggerEventId={}",
                result.resultId, amount, event.eventId);
        out.collect(result);
    }
}
