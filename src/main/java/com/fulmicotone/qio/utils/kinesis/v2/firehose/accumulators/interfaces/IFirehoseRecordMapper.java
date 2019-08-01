package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces;


import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface IFirehoseRecordMapper<I> extends BiFunction<IFirehoseStringMapper<I>, IFirehoseByteMapper, Function<List<I>, List<Record>>> {
}
