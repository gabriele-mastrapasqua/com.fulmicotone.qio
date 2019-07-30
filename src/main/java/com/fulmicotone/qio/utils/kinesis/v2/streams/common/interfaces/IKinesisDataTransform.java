package com.fulmicotone.qio.utils.kinesis.v2.streams.common.interfaces;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by enryold on 19/12/16.
 */
public interface IKinesisDataTransform<T, O> extends Function<Optional<T>, Optional<List<O>>> {

}
