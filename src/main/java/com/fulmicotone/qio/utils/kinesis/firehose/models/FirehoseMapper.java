package com.fulmicotone.qio.utils.kinesis.firehose.models;

import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IByteMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IObjectMapper;

public class FirehoseMapper<I, T> {

    private IByteMapper<T> byteMapper;
    private IObjectMapper<I, T> firehoseMapper;

    private FirehoseMapper(Builder<I, T> builder) {
        byteMapper = builder.byteMapper;
        firehoseMapper = builder.firehoseMapper;
    }



    public static Builder newBuilder() {
        return new Builder<>();
    }

    public IByteMapper<T> getByteMapper() {
        return byteMapper;
    }

    public IObjectMapper<I, T> getFirehoseMapper() {
        return firehoseMapper;
    }

    public static final class Builder<I, T> {
        private IByteMapper<T> byteMapper;
        private IObjectMapper<I, T> firehoseMapper;

        private Builder() {
        }

        public Builder withByteMapper(IByteMapper<T> val) {
            byteMapper = val;
            return this;
        }

        public Builder withFirehoseMapper(IObjectMapper<I, T> val) {
            firehoseMapper = val;
            return this;
        }

        public FirehoseMapper build() {
            return new FirehoseMapper<>(this);
        }
    }
}
