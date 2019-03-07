package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricInternalQueueSize extends GenericMetric<Integer> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.GAUGE, description = "Internal queue size", level = DataSourceLevel.INFO)
    private final AtomicInteger queueSize = new AtomicInteger(0);

    public MetricInternalQueueSize(String name, int queueNumber) {
        super(name + "-internal-queue-size-"+queueNumber, DataSourceType.GAUGE, "Internal queue size ("+queueNumber+")", DataSourceLevel.INFO);
    }

    @Override
    public Integer getValue() {
        return null;
    }

    @Override
    public void setValue(Integer value) {
        queueSize.set(value);
    }

    @Override
    public void incrementValue() {
        queueSize.incrementAndGet();
    }

    @Override
    public void incrementValue(Integer delta) {

    }

}
