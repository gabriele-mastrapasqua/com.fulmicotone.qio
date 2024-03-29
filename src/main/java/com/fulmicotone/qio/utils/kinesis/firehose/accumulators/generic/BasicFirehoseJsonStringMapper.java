package com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.interfaces.IFirehoseStringMapper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BasicFirehoseJsonStringMapper<I> implements IFirehoseStringMapper<I> {
    @Override
    public String apply(List<I> s) {
        return s.stream()
                .map(i -> {
                    try {
                        return new ObjectMapper().writeValueAsString(i)+"\n";
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.joining());
    }
}
