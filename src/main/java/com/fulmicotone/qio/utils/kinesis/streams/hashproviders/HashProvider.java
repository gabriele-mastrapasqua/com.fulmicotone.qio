package com.fulmicotone.qio.utils.kinesis.streams.hashproviders;


import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IStreamShardHelper;

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
