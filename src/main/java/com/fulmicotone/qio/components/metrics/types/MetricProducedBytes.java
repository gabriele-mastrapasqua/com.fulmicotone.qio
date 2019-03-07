package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class MetricProducedBytes extends GenericMetric<Long> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Produced bytes", level = DataSourceLevel.INFO)
    private final AtomicLong producedBytes = new AtomicLong();


    public MetricProducedBytes(String name) {
        super(name+"-produced-bytes", DataSourceType.COUNTER, "Produced bytes", DataSourceLevel.INFO);
    }

    @Override
    public Long getValue() {
        return producedBytes.get();
    }

    @Override
    public void setValue(Long value) {
        producedBytes.set(value);
    }

    @Override
    public void incrementValue() {
        producedBytes.incrementAndGet();
    }

    @Override
    public void incrementValue(Long delta) {
        producedBytes.addAndGet(delta);
    }
}
