package com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces;

import java.util.Optional;
import java.util.function.Function;

@FunctionalInterface
public interface IKinesisStreamsByteMapper extends Function<String, Optional<byte[]>> {
}
