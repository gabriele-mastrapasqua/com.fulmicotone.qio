package com.fulmicotone.qio.utils.kinesis.streams.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListDecoder;
import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListEncoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class KinesisJsonListDecoder<I> implements IKinesisListDecoder<I> {

    private CollectionType typeReference;
    private static ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<I> apply(ByteBuffer buff, Class<I> clazz) {

        if(typeReference == null){
            typeReference =
                    TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
        }

        try {
            List<I> objs;

            if(buff.hasArray())
            {
                objs = mapper.readValue(buff.array(), typeReference);
            }
            else
            {
                buff.clear();
                objs = mapper.readValue(StandardCharsets.UTF_8.decode(buff).toString(),typeReference);
            }

            return objs;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
