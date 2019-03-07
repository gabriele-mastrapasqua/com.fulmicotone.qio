package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricExecutorQueueSize extends GenericMetric<Integer> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.GAUGE, description = "Executor queue size", level = DataSourceLevel.INFO)
    private final AtomicInteger queueSize = new AtomicInteger(0);

    public MetricExecutorQueueSize(String name) {
        super(name + "-executor-queue-size", DataSourceType.GAUGE, "Executor queue size", DataSourceLevel.INFO);
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
