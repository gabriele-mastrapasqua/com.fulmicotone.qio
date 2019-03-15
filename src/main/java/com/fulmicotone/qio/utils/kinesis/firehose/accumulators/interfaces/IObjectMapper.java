package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces;

import java.util.Optional;
import java.util.function.Function;

@FunctionalInterface
public interface IObjectMapper<In, Out> extends Function<In, Optional<Out>> {
}
