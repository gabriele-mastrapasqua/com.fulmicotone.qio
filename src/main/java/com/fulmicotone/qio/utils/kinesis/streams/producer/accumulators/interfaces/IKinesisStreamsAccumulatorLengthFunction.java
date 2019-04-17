package com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.streams.producer.models.KinesisStreamsMapper;

public interface IKinesisStreamsAccumulatorLengthFunction<I> extends IQueueIOAccumulatorLengthFunction<I> {

    IKinesisStreamsAccumulatorLengthFunction<I> withFirehoseMapper(KinesisStreamsMapper<I> mapper);
}
