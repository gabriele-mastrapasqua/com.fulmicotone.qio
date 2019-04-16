package com.fulmicotone.qio.utils.kinesis.streams.accumulators;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces.IKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces.IKinesisStreamsRecordMapper;
import com.fulmicotone.qio.utils.kinesis.streams.accumulators.interfaces.IKinesisStreamsStringMapper;
import com.fulmicotone.qio.utils.kinesis.streams.models.KinesisStreamsMapper;

import java.util.List;

public class KinesisStreamsAccumulator<I> extends QueueIOAccumulator<I, Record> {

    protected KinesisStreamsMapper<I> kinesisStreamsMapper;

    public KinesisStreamsAccumulator(double byteSizeLimit, KinesisStreamsMapper<I> kinesisStreamsMapper, IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction)
    {
        super(byteSizeLimit);
        this.kinesisStreamsMapper = kinesisStreamsMapper;
        this.accumulatorLengthFunction = accumulatorLengthFunction;
    }

    @Override
    public List<Record> getRecords() {

        IKinesisStreamsStringMapper<I> stringMapper = kinesisStreamsMapper.getStringMapper();
        IKinesisStreamsByteMapper byteMapper = kinesisStreamsMapper.getByteMapper();
        IKinesisStreamsRecordMapper<I> recordMapper = kinesisStreamsMapper.getRecordMapper();

        return recordMapper.apply(stringMapper, byteMapper)
                .apply(accumulator);
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction() {
        return accumulatorLengthFunction;
    }
}
