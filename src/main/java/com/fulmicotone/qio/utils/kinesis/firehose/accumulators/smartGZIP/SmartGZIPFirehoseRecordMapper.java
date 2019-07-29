package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.smartGZIP;

import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseRecordMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import com.fulmicotone.qio.utils.kinesis.utils.FnByteCompressionMapper;
import com.fulmicotone.qio.utils.kinesis.utils.FnPrependNullBytesToByteArray;
import com.google.common.collect.Lists;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SmartGZIPFirehoseRecordMapper<I> implements IFirehoseRecordMapper<I> {


    /**
     * https://docs.aws.amazon.com/firehose/latest/dev/data-transformation.html
     * The Lambda synchronous invocation mode has a payload size limit of 6 MB for both the request and the response.
     * Request and Response must not exceed 6 MB.
     *
     * We collect elements in one record with compression ratio of 5 and max size of 1M because when it would be UNZIPPED by lambda function it would be < 6M (1M*5)
     *
     */
    public static final double RECORD_COMPRESSED_MAX_SIZE_IN_BYTES = 599_999.0;
    public static final int COMPRESSION_RATIO = 5;

    private FnByteCompressionMapper fnByteCompressionMapper = new FnByteCompressionMapper();
    private FnPrependNullBytesToByteArray fnPrependNullBytesToByteArray = new FnPrependNullBytesToByteArray();





    private Optional<Record> buildCompressedRecord(byte[] raw)
    {
        int expectedSize = (int) Math.floor(raw.length / COMPRESSION_RATIO);

        Optional<byte[]> compressed = fnByteCompressionMapper.apply(raw);

        if(!compressed.isPresent()) {
            return Optional.empty();
        }

        // IF RECORD < expectedSize I will add null bytes. That's because GZIP will compress Json 9/12 times than the original size and we need a compression ration of max 5 times.
        byte[] compressedWithNullPrefix = (compressed.get().length > expectedSize) ? compressed.get() : fnPrependNullBytesToByteArray.apply(compressed.get()).apply(expectedSize);

        Record record=Record.builder().data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(compressedWithNullPrefix))) .build();
        return Optional.of(record);
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


            int splitter = (int) Math.ceil(raw.get().length / (RECORD_COMPRESSED_MAX_SIZE_IN_BYTES *COMPRESSION_RATIO));

            return Lists.partition(is, is.size()/splitter).stream()
                    .map(l -> iFirehoseByteMapper.apply(iiFirehoseStringMapper.apply(l)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(this::buildCompressedRecord)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        };
    }
}
