package com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces;

import com.amazonaws.services.kinesisfirehose.model.Record;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface IKinesisStreamsRecordMapper<I> extends BiFunction<IKinesisStreamsStringMapper<I>, IKinesisStreamsByteMapper, Function<List<I>, List<Record>>> {
}
