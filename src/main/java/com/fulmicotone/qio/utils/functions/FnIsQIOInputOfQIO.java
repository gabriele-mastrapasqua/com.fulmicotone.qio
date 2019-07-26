package com.fulmicotone.qio.utils.functions;

import com.fulmicotone.qio.services.QueueIOService;

import java.util.Optional;
import java.util.function.BiFunction;

public class FnIsQIOInputOfQIO implements BiFunction<QueueIOService, QueueIOService, Boolean> {
    @Override
    public Boolean apply(QueueIOService queueIOService, QueueIOService queueIOService2) {

        return Optional.ofNullable(queueIOService2
                .getOutputQueues())
                .map(q -> q.containsQueue(queueIOService.getInputClass(), queueIOService.getInputQueue()))
                .orElse(false);
    }
}
