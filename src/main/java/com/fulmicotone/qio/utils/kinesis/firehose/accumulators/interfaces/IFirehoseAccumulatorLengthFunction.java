package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;

public interface IFirehoseAccumulatorLengthFunction<I> extends IQueueIOAccumulatorLengthFunction<I> {

    IFirehoseAccumulatorLengthFunction<I> withFirehoseMapper(FirehoseMapper<I> mapper);
}
