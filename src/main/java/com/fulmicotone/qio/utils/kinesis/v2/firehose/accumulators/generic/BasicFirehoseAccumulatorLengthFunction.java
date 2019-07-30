package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces.IFirehoseStringMapper;

import java.util.Collections;

public class BasicFirehoseAccumulatorLengthFunction<I> extends FirehoseAccumulatorLengthFunction<I> {

    @Override
    public Double apply(I i) {
        IFirehoseStringMapper<I> stringMapper = firehoseMapper.getStringMapper();
        IFirehoseByteMapper byteMapper = firehoseMapper.getByteMapper();

        return Double.valueOf(stringMapper
                .andThen(str -> byteMapper.apply(str).map(b -> b.length).orElse(0))
                .apply(Collections.singletonList(i)));
    }
}
