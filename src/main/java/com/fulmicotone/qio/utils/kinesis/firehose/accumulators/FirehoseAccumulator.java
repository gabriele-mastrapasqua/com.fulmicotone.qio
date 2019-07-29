package com.fulmicotone.qio.utils.kinesis.firehose.accumulators;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.FirehoseAccumulatorLengthFunction;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseMapper;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;

public class FirehoseAccumulator<I> extends QueueIOAccumulator<I, Record> {

    protected FirehoseMapper<I> firehoseMapper;

    public FirehoseAccumulator(double byteSizeLimit, FirehoseMapper<I> firehoseMapper, IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction)
    {
        super(byteSizeLimit);
        this.firehoseMapper = firehoseMapper;
        this.accumulatorLengthFunction = accumulatorLengthFunction;
    }

    @Override
    public List<Record> getRecords() {

        IFirehoseStringMapper<I> stringMapper = firehoseMapper.getStringMapper();
        IFirehoseByteMapper byteMapper = firehoseMapper.getByteMapper();
        IFirehoseRecordMapper<I> recordMapper = firehoseMapper.getRecordMapper();

        return recordMapper.apply(stringMapper, byteMapper)
                .apply(accumulator);
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<I> accumulatorLengthFunction() {
        return accumulatorLengthFunction;
    }
}
