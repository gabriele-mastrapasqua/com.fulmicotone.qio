package com.fulmicotone.qio.utils.kinesis.utils;

import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public class FnRemoveNullBytesFromByteArray implements Function<byte[], byte[]> {


    @Override
    public byte[] apply(byte[] bytes) {

        if (bytes.length <= 1)
            return bytes;

        int idx;

        for(idx = 1; idx<bytes.length; idx++)
        {
            int head = ((int) bytes[idx-1] & 0xff) | ((bytes[idx] << 8 ) & 0xff00 );
            if(GZIPInputStream.GZIP_MAGIC == head)
                break;
        }

        int newLength = bytes.length-(idx-1);
        byte[] result = new byte[newLength];

        System.arraycopy(bytes, (idx-1), result, 0, newLength);
        return result;

    }
}
