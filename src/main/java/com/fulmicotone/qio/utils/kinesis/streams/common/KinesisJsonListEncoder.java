package com.fulmicotone.qio.utils.kinesis.streams.common;

import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class KinesisJsonListEncoder<I> implements IKinesisListEncoder<I> {
    @Override
    public ByteBuffer apply(List<I> is) {
        return null;
    }
}
