package com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface IKinesisStreamsStringMapper<In> extends Function<List<In>, String> {
}
