package com.fulmicotone.qio.components.metrics;

import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class ElementsProducedMetric extends GenericMetric<Integer> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.COUNTER, description = "Produced object", level = DataSourceLevel.INFO)
    private final AtomicInteger producedObject = new AtomicInteger();


    public ElementsProducedMetric(String name) {
        super(name+"-produced-objects", DataSourceType.COUNTER, "Produced object", DataSourceLevel.INFO);
    }

    @Override
    public String logMetric() {
        return "produced "+producedObject.get()+" elements";
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
