package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.smartGZIP;

import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.generic.FirehoseAccumulatorLengthFunction;

import java.util.Collections;

import static com.fulmicotone.qio.utils.kinesis.firehose.accumulators.smartGZIP.SmartGZIPFirehoseRecordMapper.COMPRESSION_RATIO;

public class SmartGZIPFirehoseAccumulatorLengthFunction<I> extends FirehoseAccumulatorLengthFunction<I> {

    @Override
    public Double apply(I i) {
        double size = firehoseMapper.getStringMapper().apply(Collections.singletonList(i)).length();
        return Math.ceil(size / COMPRESSION_RATIO);
    }
}
