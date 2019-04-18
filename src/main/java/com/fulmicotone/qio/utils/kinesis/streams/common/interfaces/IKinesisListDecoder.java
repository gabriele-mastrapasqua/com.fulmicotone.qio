package com.fulmicotone.qio.utils.kinesis.streams.common.interfaces;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiFunction;

@FunctionalInterface
public interface IKinesisListDecoder<I> extends BiFunction<ByteBuffer, Class<I>, List<I>> {
}
