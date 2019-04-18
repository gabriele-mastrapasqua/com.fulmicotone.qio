package com.fulmicotone.qio.interfaces;

import java.util.function.Function;

@FunctionalInterface
public interface IQueueIOTransform<I, T> extends Function<I, T> {
}
