package com.fulmicotone.qio.components.metrics;

import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class ElementsReceivedMetric extends GenericMetric<Long> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Received object", level = DataSourceLevel.INFO)
    private final AtomicLong producedObject = new AtomicLong();


    public ElementsReceivedMetric(String name) {
        super(name+"-received-objects", DataSourceType.COUNTER, "Received object", DataSourceLevel.INFO);
    }

    @Override
    public String logMetric() {
        return "received "+producedObject.get()+" elements";
    }

    @Override
    public Long getValue() {
        return producedObject.get();
    }

    @Override
    public void setValue(Long value) {
        producedObject.set(value);
    }

    @Override
    public void incrementValue() {
        producedObject.incrementAndGet();
    }

    @Override
    public void incrementValue(Long delta) {
        producedObject.addAndGet(delta);
    }
}
