package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces;

import java.util.Optional;
import java.util.function.Function;

@FunctionalInterface
public interface IFirehoseByteMapper extends Function<String, Optional<byte[]>> {
}
