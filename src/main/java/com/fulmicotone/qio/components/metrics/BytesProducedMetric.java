package com.fulmicotone.qio.components.metrics;

import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class BytesProducedMetric extends GenericMetric<Long> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Produced bytes", level = DataSourceLevel.INFO)
    private final AtomicLong producedBytes = new AtomicLong();


    public BytesProducedMetric(String name) {
        super(name+"-produced-bytes", DataSourceType.COUNTER, "Produced bytes", DataSourceLevel.INFO);
    }

    @Override
    public String logMetric() {
        return "produced "+ producedBytes.get()+" bytes";
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
