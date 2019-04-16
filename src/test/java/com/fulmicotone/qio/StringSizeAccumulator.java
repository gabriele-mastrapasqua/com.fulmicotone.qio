package com.fulmicotone.qio;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;

import java.util.List;

public class StringSizeAccumulator extends QueueIOAccumulator<String, String> {


    public StringSizeAccumulator(double byteSizeLimit) {
        super(byteSizeLimit);
    }

    @Override
    public List<String> getRecords() {
        return accumulator;
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<String> accumulatorLengthFunction() {
        return s -> (double) s.getBytes().length;
    }
}
