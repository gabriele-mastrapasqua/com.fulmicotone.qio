package com.fulmicotone.qio.utils.kinesis.interfaces;

import java.util.Optional;
import java.util.function.Function;

@FunctionalInterface
public interface ICompressionMapper extends Function<byte[],  Optional<byte[]>> {
}
