package com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.streams.models.KinesisStreamsMapper;

public interface IKinesisStreamsAccumulatorLengthFunction<I> extends IQueueIOAccumulatorLengthFunction<I> {

    IKinesisStreamsAccumulatorLengthFunction<I> withFirehoseMapper(KinesisStreamsMapper<I> mapper);
}
