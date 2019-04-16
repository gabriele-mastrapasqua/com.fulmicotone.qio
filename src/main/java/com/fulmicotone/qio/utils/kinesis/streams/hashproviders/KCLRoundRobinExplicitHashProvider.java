package com.fulmicotone.qio.utils.kinesis.streams.hashproviders;


import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.utils.ShardHelper;

public class KCLRoundRobinExplicitHashProvider implements IExplicitHashProvider {


    private final ShardHelper shardHelper;


    public KCLRoundRobinExplicitHashProvider(ShardHelper shardHelper)
    {
        this.shardHelper = shardHelper;
    }

    public String nextHashKeyForStream(String streamName){
        return shardHelper.nextKeyHelperForStream(streamName).generateRandomKey();
    }

    public KCLShardKeyHelper nextKeyHelperForStream(String streamName){
        return shardHelper.nextKeyHelperForStream(streamName);
    }
}
