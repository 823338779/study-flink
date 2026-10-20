package com.example.payment.consumer;

import com.example.payment.message.ShopTotalDTO;
import com.example.payment.stat.ShopStatRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Listener通过代理调用本服务：去重登记和统计写入必须一起提交或回滚。 */
@Service
public class ResultProcessingService {
    private final ShopStatRepository repository;
    public ResultProcessingService(ShopStatRepository repository) {
        this.repository = repository;
    }
    @Transactional
    public ProcessingOutcomeEnum process(ShopTotalDTO event) {
        event.validate();
        if (!repository.insertProcessed(event)) {
            return ProcessingOutcomeEnum.DUPLICATE;
        }
        return repository.upsertIfNewer(event) ? ProcessingOutcomeEnum.APPLIED : ProcessingOutcomeEnum.STALE;
    }
}
