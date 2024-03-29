package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;

public abstract class FirehoseAccumulatorLengthFunction<I> implements IQueueIOAccumulatorLengthFunction<I>, IFirehoseAccumulatorLengthFunction<I> {

    protected FirehoseMapper<I> firehoseMapper;

    @Override
    public IFirehoseAccumulatorLengthFunction<I> withFirehoseMapper(FirehoseMapper<I> mapper) {
        this.firehoseMapper = mapper;
        return this;
    }


}
