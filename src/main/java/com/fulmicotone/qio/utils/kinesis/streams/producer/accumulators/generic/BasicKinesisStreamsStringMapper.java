package com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.interfaces.IKinesisStreamsStringMapper;

import java.util.List;

public class BasicKinesisStreamsStringMapper<I> implements IKinesisStreamsStringMapper<I> {
    @Override
    public String apply(List<I> s) {
        try {
            return new ObjectMapper().writeValueAsString(s);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
