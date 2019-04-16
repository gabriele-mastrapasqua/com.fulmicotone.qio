package com.fulmicotone.qio.utils.kinesis.utils;

import com.amazonaws.util.IOUtils;
import com.fulmicotone.qio.utils.kinesis.interfaces.IDecompressionMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public class FnByteDecompressionMapper<T> implements IDecompressionMapper {


    @Override
    public Optional<byte[]> apply(byte[] bytes) {


        ByteArrayInputStream byteStream;
        GZIPInputStream zipStream;

        try
        {
            byteStream = new ByteArrayInputStream(bytes);
            try
            {
                zipStream = new GZIPInputStream(byteStream);
            }
            finally
            {
                byteStream.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return Optional.empty();
        }

        try {

            byte[] result = IOUtils.toByteArray(zipStream);

            return Optional.of(result);
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }


    }


}
