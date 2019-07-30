package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.v2.firehose.models.FirehoseMapper;

public interface IFirehoseAccumulatorLengthFunction<I> extends IQueueIOAccumulatorLengthFunction<I> {

    IFirehoseAccumulatorLengthFunction<I> withFirehoseMapper(FirehoseMapper<I> mapper);
}
