package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.functions;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;

public class SmartGZIPAccumulatorLengthFunction<I, T> implements IQueueIOAccumulatorLengthFunction<I> {

    private FirehoseMapper<I, T> mapper;

    public SmartGZIPAccumulatorLengthFunction(FirehoseMapper<I, T> mapper){
        this.mapper = mapper;
    }

    @Override
    public Integer apply(I i) {
        return null;
    }
}
