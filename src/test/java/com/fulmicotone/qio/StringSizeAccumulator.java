package com.fulmicotone.qio;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;

public class StringSizeAccumulator extends QueueIOAccumulator<String> {


    public StringSizeAccumulator(double byteSizeLimit) {
        super(byteSizeLimit);
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<String> accumulatorLengthFunction() {
        return s -> s.getBytes().length;
    }
}
