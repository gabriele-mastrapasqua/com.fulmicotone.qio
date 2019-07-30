package com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.generic;

import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces.IFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import com.google.common.collect.Lists;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BasicFirehoseRecordMapper<I> implements IFirehoseRecordMapper<I> {


    public static final double RECORD_MAX_SIZE_IN_BYTES = 999_999.0;


    private Record buildRecord(byte[] raw)
    {
        Record record = Record
                .builder()
                .data(
                        SdkBytes.fromByteBuffer(ByteBuffer.wrap(raw))) .build();
        return record;
    }

    @Override
    public Function<List<I>, List<Record>> apply(IFirehoseStringMapper<I> iiFirehoseStringMapper, IFirehoseByteMapper iFirehoseByteMapper) {


        return is -> {

            if(is == null) { return new ArrayList<>(); }
            if(is.size() == 0) { return new ArrayList<>(); }


            Optional<byte[]> raw = iFirehoseByteMapper.apply(iiFirehoseStringMapper.apply(is));

            if(!raw.isPresent())
            {
                return new ArrayList<>();
            }


            int splitter = (int) Math.ceil(raw.get().length / RECORD_MAX_SIZE_IN_BYTES);

            return Lists.partition(is, is.size()/splitter).stream()
                    .map(l -> iFirehoseByteMapper.apply(iiFirehoseStringMapper.apply(l)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(this::buildRecord)
                    .collect(Collectors.toList());
        };
    }
}
