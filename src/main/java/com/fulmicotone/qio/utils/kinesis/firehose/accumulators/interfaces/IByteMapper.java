package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public interface IByteMapper<I> extends Function<List<I>, Optional<byte[]>> {
}
