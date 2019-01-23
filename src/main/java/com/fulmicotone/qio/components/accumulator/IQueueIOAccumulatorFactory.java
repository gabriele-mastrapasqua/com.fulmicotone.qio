package com.fulmicotone.qio.components.accumulator;

public interface IQueueIOAccumulatorFactory<I>  {


    IQueueIOAccumulator<I> build();
}
