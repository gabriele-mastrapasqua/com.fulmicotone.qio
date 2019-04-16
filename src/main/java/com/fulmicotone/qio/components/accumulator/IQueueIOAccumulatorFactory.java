package com.fulmicotone.qio.components.accumulator;

public interface IQueueIOAccumulatorFactory<I, T>  {


    IQueueIOAccumulator<I, T> build();
}
