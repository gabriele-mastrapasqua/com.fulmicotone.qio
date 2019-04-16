package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.utils;


import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.KCLShardKeyHelper;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.Iterators.cycle;

public class ShardHelper {

    private Map<String, Iterator<KCLShardKeyHelper>> shardKeyHelpers = new ConcurrentHashMap<>();

    public void putShardKeyHelpers(String streamName, Set<KCLShardKeyHelper> helpers){
        shardKeyHelpers.put(streamName, cycle(helpers));
    }

    public KCLShardKeyHelper nextKeyHelperForStream(String streamName){
        return nextKeyHelperForStreamSync(shardKeyHelpers.get(streamName));
    }

    private synchronized KCLShardKeyHelper nextKeyHelperForStreamSync(Iterator<KCLShardKeyHelper> iterator){
        return iterator.next();
    }
}
