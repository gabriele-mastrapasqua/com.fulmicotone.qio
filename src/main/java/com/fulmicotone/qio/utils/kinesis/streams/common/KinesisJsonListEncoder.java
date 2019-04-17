package com.fulmicotone.qio.utils.kinesis.streams.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListEncoder;

import java.util.List;

public class KinesisJsonListEncoder<I> implements IKinesisListEncoder<I> {
    @Override
    public String apply(List<I> is) {
        try {
            return new ObjectMapper().writeValueAsString(is);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
