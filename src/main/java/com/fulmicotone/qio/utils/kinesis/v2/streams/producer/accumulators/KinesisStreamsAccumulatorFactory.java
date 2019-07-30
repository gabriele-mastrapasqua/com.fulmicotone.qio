package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulator;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.generic.BasicKinesisStreamsAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.generic.BasicKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.generic.BasicKinesisStreamsRecordMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.generic.KinesisStreamsAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsRecordMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.accumulators.interfaces.IKinesisStreamsStringMapper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.models.KinesisStreamsMapper;

import java.nio.ByteBuffer;

public class KinesisStreamsAccumulatorFactory<I> implements IQueueIOAccumulatorFactory<I, ByteBuffer> {

    private IKinesisStreamsStringMapper<I> stringMapper;
    private IKinesisStreamsRecordMapper recordMapper;
    private IKinesisStreamsByteMapper byteMapper;
    private KinesisStreamsAccumulatorLengthFunction<I> accumulatorLengthFunction;
    private double byteSizeLimit;

    public KinesisStreamsAccumulatorFactory(double byteSizeLimit, IKinesisStreamsStringMapper<I> stringMapper, IKinesisStreamsRecordMapper recordMapper, IKinesisStreamsByteMapper byteMapper, KinesisStreamsAccumulatorLengthFunction<I> accumulatorLengthFunction){
        this.stringMapper = stringMapper;
        this.recordMapper = recordMapper;
        this.byteMapper = byteMapper;
        this.byteSizeLimit = byteSizeLimit;
        this.accumulatorLengthFunction = accumulatorLengthFunction;
    }


    public static <I> KinesisStreamsAccumulatorFactory<I> getBasicRecordFactory(double byteSizeLimit, IKinesisStreamsStringMapper<I> stringMapper){
        return new KinesisStreamsAccumulatorFactory<>(byteSizeLimit, stringMapper, new BasicKinesisStreamsRecordMapper(), new BasicKinesisStreamsByteMapper(), new BasicKinesisStreamsAccumulatorLengthFunction<>());
    }



    @Override
    public IQueueIOAccumulator<I, ByteBuffer> build() {

        KinesisStreamsMapper mapper = KinesisStreamsMapper.newBuilder()
                .withByteMapper(byteMapper)
                .withRecordMapper(recordMapper)
                .withStringMapper(stringMapper)
                .build();
        accumulatorLengthFunction
                .withFirehoseMapper(mapper);

        return new KinesisStreamsAccumulator<I>(byteSizeLimit, mapper, accumulatorLengthFunction);

    }
}
