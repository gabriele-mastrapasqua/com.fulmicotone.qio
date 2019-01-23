package com.fulmicotone.qio.components.metrics;

import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class QueueSizeMetric extends GenericMetric<Integer>{

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.GAUGE, description = "Queue size", level = DataSourceLevel.INFO)
    private final AtomicInteger queueSize = new AtomicInteger(0);

    public QueueSizeMetric(String name) {
        super(name + "-queue-size", DataSourceType.GAUGE, "Queue size", DataSourceLevel.INFO);
    }

    @Override
    public String logMetric() {
        return "Queue: " + name + " elements: " + queueSize.get();
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
