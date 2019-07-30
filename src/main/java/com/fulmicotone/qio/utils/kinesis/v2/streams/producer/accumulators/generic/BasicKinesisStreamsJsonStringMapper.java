package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.streams.common.KinesisJsonListEncoder;
import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListEncoder;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsStringMapper;

import java.util.List;

public class BasicKinesisStreamsJsonStringMapper<I> implements IKinesisStreamsStringMapper<I> {

    IKinesisListEncoder<I> encoder = new KinesisJsonListEncoder<>();

    @Override
    public String apply(List<I> s) {
        return encoder.apply(s);
    }
}
