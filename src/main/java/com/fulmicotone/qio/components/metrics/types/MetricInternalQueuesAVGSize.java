package com.fulmicotone.qio.components.metrics.types;

import com.fulmicotone.qio.components.metrics.generics.GenericMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.annotations.MonitorTags;
import com.netflix.servo.tag.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricInternalQueuesAVGSize extends GenericMetric<Integer> {

    @Monitor(name = "PLACEHOLDER", type = DataSourceType.GAUGE, description = "Internal queue size", level = DataSourceLevel.INFO)
    private final AtomicInteger queueSize = new AtomicInteger(0);


    public MetricInternalQueuesAVGSize(String name) {
        super(name + "-internal-queues-avg-size", DataSourceType.GAUGE, "Internal queues avg size", DataSourceLevel.INFO);
    }

    @Override
    public Integer getValue() {
        return queueSize.intValue();
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
        queueSize.getAndAdd(delta);
    }

}
