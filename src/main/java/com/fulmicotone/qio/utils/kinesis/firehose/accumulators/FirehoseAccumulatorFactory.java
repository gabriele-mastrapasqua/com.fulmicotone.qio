package com.fulmicotone.qio.utils.kinesis.firehose.accumulators;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulator;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.BasicFirehoseAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.BasicFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.FirehoseAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.smartGZIP.SmartGZIPFirehoseAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.smartGZIP.SmartGZIPFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.BasicFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;
import software.amazon.awssdk.services.firehose.model.Record;

public class FirehoseAccumulatorFactory<I> implements IQueueIOAccumulatorFactory<I, Record> {

    private IFirehoseStringMapper<I> stringMapper;
    private IFirehoseRecordMapper recordMapper;
    private IFirehoseByteMapper byteMapper;
    private FirehoseAccumulatorLengthFunction<I> accumulatorLengthFunction;
    private double byteSizeLimit;

    public FirehoseAccumulatorFactory(double byteSizeLimit, IFirehoseStringMapper<I> stringMapper, IFirehoseRecordMapper recordMapper, IFirehoseByteMapper byteMapper, FirehoseAccumulatorLengthFunction<I> firehoseAccumulatorLengthFunction){
        this.stringMapper = stringMapper;
        this.recordMapper = recordMapper;
        this.byteMapper = byteMapper;
        this.byteSizeLimit = byteSizeLimit;
        this.accumulatorLengthFunction = firehoseAccumulatorLengthFunction;
    }


    public static <I>FirehoseAccumulatorFactory<I> getSmartGZIPFactory(double byteSizeLimit, IFirehoseStringMapper<I> stringMapper){
        return new FirehoseAccumulatorFactory<>(byteSizeLimit, stringMapper, new SmartGZIPFirehoseRecordMapper(), new BasicFirehoseByteMapper(), new SmartGZIPFirehoseAccumulatorLengthFunction<>());
    }

    public static <I>FirehoseAccumulatorFactory<I> getBasicRecordFactory(double byteSizeLimit, IFirehoseStringMapper<I> stringMapper){
        return new FirehoseAccumulatorFactory<>(byteSizeLimit, stringMapper, new BasicFirehoseRecordMapper(), new BasicFirehoseByteMapper(), new BasicFirehoseAccumulatorLengthFunction<>());
    }



    @Override
    public IQueueIOAccumulator<I, Record> build() {

        FirehoseMapper mapper = FirehoseMapper.newBuilder()
                .withByteMapper(byteMapper)
                .withRecordMapper(recordMapper)
                .withStringMapper(stringMapper)
                .build();
        accumulatorLengthFunction
                .withFirehoseMapper(mapper);

        return new FirehoseAccumulator<I>(byteSizeLimit, mapper, accumulatorLengthFunction);

    }
}
