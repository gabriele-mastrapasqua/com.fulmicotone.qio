package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsRecordMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsStringMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.models.KinesisStreamsMapper;

import java.nio.ByteBuffer;
import java.util.List;

public class KinesisStreamsAccumulator<I> extends QueueIOAccumulator<I, ByteBuffer> {

    protected KinesisStreamsMapper<I> kinesisStreamsMapper;

    public KinesisStreamsAccumulator(double byteSizeLimit, KinesisStreamsMapper<I> kinesisStreamsMapper, IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction)
    {
        super(byteSizeLimit);
        this.kinesisStreamsMapper = kinesisStreamsMapper;
        this.accumulatorLengthFunction = accumulatorLengthFunction;
    }

    @Override
    public List<ByteBuffer> getRecords() {

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
