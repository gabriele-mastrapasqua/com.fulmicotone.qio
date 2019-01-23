package com.fulmicotone.qio.components.accumulator;

import java.util.function.Function;

@FunctionalInterface
public interface IQueueIOAccumulatorLengthFunction<I> extends Function<I, Integer> {
}
