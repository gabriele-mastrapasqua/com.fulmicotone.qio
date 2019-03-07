package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class MetricReceivedBytes extends GenericMetric<Long> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Queue size", level = DataSourceLevel.INFO)
    private final AtomicLong receivedObject = new AtomicLong();

    public MetricReceivedBytes(String name) {
        super(name+"-received-objects", DataSourceType.COUNTER, "Received object", DataSourceLevel.INFO);
    }

    @Override
    public Long getValue() {
        return receivedObject.get();
    }

    @Override
    public void setValue(Long value) {
        receivedObject.set(value);
    }

    @Override
    public void incrementValue() {
        receivedObject.incrementAndGet();
    }

    @Override
    public void incrementValue(Long delta) {
        receivedObject.addAndGet(delta);
    }


}
