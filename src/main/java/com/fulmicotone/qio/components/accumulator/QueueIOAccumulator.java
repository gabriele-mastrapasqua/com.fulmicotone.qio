package com.fulmicotone.qio.components.accumulator;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.ArrayList;
import java.util.List;

public abstract class QueueIOAccumulator<E> implements IQueueIOAccumulator<E> {

    private double byteSizeLimit;
    private List<E> accumulator = new ArrayList<>();
    private AtomicDouble accumulatorSize = new AtomicDouble(0);
    private final IQueueIOAccumulatorLengthFunction<E> accumulatorLengthFunction;

    public QueueIOAccumulator(double byteSizeLimit){
        this.byteSizeLimit = byteSizeLimit;
        this.accumulatorLengthFunction = accumulatorLengthFunction();
    }


    @Override
    public void add(E obj) {

        double size = objectSize(obj);

        if(shouldBecomeFull(size)){
            return;
        }

        accumulator.add(obj);
        accumulatorSize.addAndGet(size);
    }

    @Override
    public List<E> getRecords() {
        return accumulator;
    }

    @Override
    public boolean hasSpaceAvailable() {
        return (accumulatorSize.get() < byteSizeLimit);
    }


    private double objectSize(E obj)
    {
        return accumulatorLengthFunction.apply(obj);
    }

    private boolean shouldBecomeFull(double size){
        return (accumulatorSize.get() + size > byteSizeLimit);
    }
}
