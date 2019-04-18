package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricProducedElements extends GenericMetric<Integer> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Produced object", level = DataSourceLevel.INFO)
    private final AtomicInteger producedObject = new AtomicInteger();


    public MetricProducedElements(String name) {
        super(name+"-produced-objects", DataSourceType.COUNTER, "Produced object", DataSourceLevel.INFO);
    }

    @Override
    public Integer getValue() {
        return producedObject.get();
    }

    @Override
    public void setValue(Integer value) {
        producedObject.set(value);
    }

    @Override
    public void incrementValue() {
        producedObject.incrementAndGet();
    }

    @Override
    public void incrementValue(Integer delta) {
        producedObject.addAndGet(delta);
    }




}
