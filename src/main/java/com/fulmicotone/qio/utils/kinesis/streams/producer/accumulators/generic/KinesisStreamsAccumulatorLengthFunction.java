package com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces.IKinesisStreamsAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.streams.producer.models.KinesisStreamsMapper;

public abstract class KinesisStreamsAccumulatorLengthFunction<I> implements IQueueIOAccumulatorLengthFunction<I>, IKinesisStreamsAccumulatorLengthFunction<I> {

    protected KinesisStreamsMapper<I> kinesisStreamsMapper;

    @Override
    public IKinesisStreamsAccumulatorLengthFunction<I> withFirehoseMapper(KinesisStreamsMapper<I> mapper) {
        this.kinesisStreamsMapper = mapper;
        return this;
    }


}
