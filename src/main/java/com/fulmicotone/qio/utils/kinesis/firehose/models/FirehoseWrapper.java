package com.fulmicotone.qio.utils.kinesis.firehose.models;

import com.amazonaws.services.kinesisfirehose.model.Record;

import java.util.List;

public class FirehoseWrapper<T> {

    private T obj;
    private List<Record> transformedRecords;

    private FirehoseWrapper(Builder<T> builder) {
        obj = builder.obj;
        transformedRecords = builder.transformedRecords;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<Record> getTransformedRecords() {
        return transformedRecords;
    }

    public T getObj() {
        return obj;
    }


    public static final class Builder<T> {
        private T obj;
        private List<Record> transformedRecords;

        private Builder() {
        }

        public Builder withObj(T val) {
            obj = val;
            return this;
        }

        public Builder withTransformedRecords(List<Record> val) {
            transformedRecords = val;
            return this;
        }

        public FirehoseWrapper build() {
            return new FirehoseWrapper<>(this);
        }
    }
}
