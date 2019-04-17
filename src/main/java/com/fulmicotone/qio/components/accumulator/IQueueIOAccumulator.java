package com.fulmicotone.qio.components.accumulator;

import java.util.List;
import java.util.Optional;

public interface IQueueIOAccumulator<I, T>  {


    boolean add(I obj);
    List<T> getRecords();
    IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction();

}
