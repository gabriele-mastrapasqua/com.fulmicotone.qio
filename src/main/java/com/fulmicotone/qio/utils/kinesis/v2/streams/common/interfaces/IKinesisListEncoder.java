package com.fulmicotone.qio.utils.kinesis.v2.streams.common.interfaces;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface IKinesisListEncoder<I> extends Function<List<I>, String> {
}
