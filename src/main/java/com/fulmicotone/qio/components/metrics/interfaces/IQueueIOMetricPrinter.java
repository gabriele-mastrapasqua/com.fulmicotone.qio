package com.fulmicotone.qio.components.metrics.interfaces;


import com.fulmicotone.qio.components.metrics.QueueIOMetric;

import java.util.function.Function;

@FunctionalInterface
public interface IQueueIOMetricPrinter extends Function<QueueIOMetric, String> {
}
