package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces.IFirehoseByteMapper;

import java.util.Optional;

public class BasicFirehoseByteMapper implements IFirehoseByteMapper {
    @Override
    public Optional<byte[]> apply(String s) {
        return Optional.of(s.getBytes());
    }
}
