package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces;

import java.util.Set;

public interface IStreamShardHelper {

    String getStreamName();
    IStreamShardHelper withHelpers(Set<IExplicitShardKeyHelper> helpers);
    IStreamShardHelper withStreamName(String streamName);
    IExplicitShardKeyHelper keyHelperForStream();
}
