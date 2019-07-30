package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders;


import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.interfaces.IExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.interfaces.IStreamShardHelper;

public class HashProvider implements IExplicitHashProvider {


    private final IStreamShardHelper streamShardHelper;


    public HashProvider(IStreamShardHelper streamShardHelper)
    {
        this.streamShardHelper = streamShardHelper;
    }

    @Override
    public String nextHashKey() {
        return streamShardHelper.keyHelperForStream().generateHashKey();
    }
}
