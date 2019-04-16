package com.fulmicotone.qio.components.accumulator;

import java.util.List;
import java.util.Optional;

public interface IQueueIOAccumulator<I, T>  {


    void add(I obj);
    List<T> getRecords();
    boolean hasSpaceAvailable();
    IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction();

}
