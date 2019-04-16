package com.fulmicotone.qio.utils.kinesis.utils;


import com.fulmicotone.qio.utils.kinesis.interfaces.ICompressionMapper;

import java.io.ByteArrayOutputStream;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;

public class FnByteCompressionMapper implements ICompressionMapper {


    @Override
    public Optional<byte[]> apply(byte[] bytes) {

        byte[] dataToCompress = bytes;
        ByteArrayOutputStream byteStream;


        try
        {
            byteStream = new ByteArrayOutputStream(dataToCompress.length);
            try
            {
                GZIPOutputStream zipStream =
                        new GZIPOutputStream(byteStream);
                try
                {
                    zipStream.write(dataToCompress);
                }
                finally
                {
                    zipStream.close();
                }
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

        return Optional.of(byteStream.toByteArray());
    }

}
