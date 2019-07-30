package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface IFirehoseStringMapper<In> extends Function<List<In>, String> {
}
