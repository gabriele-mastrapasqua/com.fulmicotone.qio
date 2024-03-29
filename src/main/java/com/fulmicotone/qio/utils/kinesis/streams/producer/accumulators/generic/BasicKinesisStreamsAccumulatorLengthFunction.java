package com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListDecoder;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces.IKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces.IKinesisStreamsStringMapper;

import java.util.Collections;

public class BasicKinesisStreamsAccumulatorLengthFunction<I> extends KinesisStreamsAccumulatorLengthFunction<I> {


    @Override
    public Double apply(I i) {
        IKinesisStreamsStringMapper<I> stringMapper = kinesisStreamsMapper.getStringMapper();
        IKinesisStreamsByteMapper byteMapper = kinesisStreamsMapper.getByteMapper();

        return Double.valueOf(stringMapper
                .andThen(str -> byteMapper.apply(str).map(b -> b.length).orElse(0))
                .apply(Collections.singletonList(i)));
    }
}
