package com.fulmicotone.qio.components.metrics.interfaces;

public interface IMetric<I> {

    void register(String nameSpace);
    String logMetric();
    I getValue();
    void setValue(I value);
    void incrementValue();
    void incrementValue(I delta);
}
