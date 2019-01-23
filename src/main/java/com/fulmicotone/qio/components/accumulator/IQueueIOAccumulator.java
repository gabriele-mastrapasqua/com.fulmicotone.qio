package com.fulmicotone.qio.components.accumulator;

import java.util.List;
import java.util.Optional;

public interface IQueueIOAccumulator<I>  {


    void add(I obj);
    List<I> getRecords();
    boolean hasSpaceAvailable();
    IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction();

}
