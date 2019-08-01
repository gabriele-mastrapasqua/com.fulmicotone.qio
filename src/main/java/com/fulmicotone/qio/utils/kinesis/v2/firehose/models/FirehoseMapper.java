package com.fulmicotone.qio.utils.kinesis.v2.firehose.models;

import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;
import com.fulmicotone.qio.utils.kinesis.v2.firehose.accumulators.interfaces.IFirehoseRecordMapper;

public class FirehoseMapper<I> {

    private IFirehoseStringMapper<I> stringMapper;
    private IFirehoseRecordMapper<I> recordMapper;
    private IFirehoseByteMapper byteMapper;

    private FirehoseMapper(Builder builder) {
        stringMapper = builder.stringMapper;
        recordMapper = builder.recordMapper;
        byteMapper = builder.byteMapper;
    }

    public static Builder newBuilder() {
        return new Builder<>();
    }

    public IFirehoseStringMapper<I> getStringMapper() {
        return stringMapper;
    }

    public IFirehoseRecordMapper<I> getRecordMapper() {
        return recordMapper;
    }

    public IFirehoseByteMapper getByteMapper() {
        return byteMapper;
    }

    public static final class Builder<I> {
        private IFirehoseStringMapper<I> stringMapper;
        private IFirehoseRecordMapper<I> recordMapper;
        private IFirehoseByteMapper byteMapper;

        private Builder() {
        }

        public Builder withStringMapper(IFirehoseStringMapper<I> val) {
            stringMapper = val;
            return this;
        }

        public Builder withRecordMapper(IFirehoseRecordMapper<I> val) {
            recordMapper = val;
            return this;
        }

        public Builder withByteMapper(IFirehoseByteMapper val) {
            byteMapper = val;
            return this;
        }

        public FirehoseMapper build() {
            return new FirehoseMapper(this);
        }
    }
}
