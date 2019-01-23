package com.fulmicotone.qio.models;

import com.fulmicotone.qio.components.metrics.QueueSizeMetric;

import java.util.Optional;
import java.util.concurrent.LinkedTransferQueue;

public class QueueIOQ<E> extends LinkedTransferQueue<E> {


    private QueueSizeMetric metric;

    public QueueIOQ(){
    }

    public QueueIOQ(QueueIOService<E> queueIOService){
        metric = new QueueSizeMetric(queueIOService.getUniqueKey());
    }

    public void registerMetric(String nameSpace){
        Optional.ofNullable(metric).ifPresent(m -> m.register(nameSpace));
    }
}
