package com.fulmicotone.qio.models;

import com.fulmicotone.qio.components.metrics.types.MetricInputQueueSize;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class QueueIOQ<E> extends LinkedBlockingQueue<E> {


    private MetricInputQueueSize metric;

    public QueueIOQ(){
        super();
    }

    public QueueIOQ(int capacity){
        super(capacity);
    }

    public QueueIOQ(QueueIOService<E, ?> queueIOService){
        metric = new MetricInputQueueSize(queueIOService.getUniqueKey());
    }

    public void registerMetric(String nameSpace){
        Optional.ofNullable(metric).ifPresent(m -> m.register(nameSpace));
    }
}
