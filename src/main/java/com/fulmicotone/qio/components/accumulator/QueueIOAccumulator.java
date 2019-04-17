package com.fulmicotone.qio.components.accumulator;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.ArrayList;
import java.util.List;

public abstract class QueueIOAccumulator<E, T> implements IQueueIOAccumulator<E, T> {

    private double byteSizeLimit;
    protected List<E> accumulator = new ArrayList<>();
    private AtomicDouble accumulatorSize = new AtomicDouble(0);
    protected IQueueIOAccumulatorLengthFunction<E> accumulatorLengthFunction;

    public QueueIOAccumulator(double byteSizeLimit){
        this.byteSizeLimit = byteSizeLimit;
        this.accumulatorLengthFunction = accumulatorLengthFunction();
    }


    @Override
    public boolean add(E obj) {

        double size = objectSize(obj);

        if(shouldBecomeFull(size)){
            return false;
        }

        accumulator.add(obj);
        accumulatorSize.addAndGet(size);
        return true;
    }


    private double objectSize(E obj)
    {
        return accumulatorLengthFunction.apply(obj);
    }

    private boolean shouldBecomeFull(double size){
        return (accumulatorSize.get() + size > byteSizeLimit);
    }
}
