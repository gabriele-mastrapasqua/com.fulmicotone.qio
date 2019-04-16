package com.fulmicotone.qio.utils.kinesis.streams.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces.IKinesisStreamsByteMapper;

import java.util.Optional;

public class BasicKinesisStreamsByteMapper implements IKinesisStreamsByteMapper {
    @Override
    public Optional<byte[]> apply(String s) {
        return Optional.of(s.getBytes());
    }
}
