package com.fulmicotone.qio.utils.kinesis.firehose.accumulators;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;

public class FirehoseQIOAccumulator<I, T> extends QueueIOAccumulator<I> {

    private FirehoseMapper<I, T> firehoseMapper;

    public FirehoseQIOAccumulator(double byteSizeLimit, FirehoseMapper<I, T> firehoseMapper) {
        super(byteSizeLimit);
        this.firehoseMapper = firehoseMapper;
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction() {
        return null;
    }
}
