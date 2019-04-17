package com.fulmicotone.qio.utils.kinesis.streams.common.interfaces;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface IKinesisListEncoder<I> extends Function<List<I>, String> {
}
