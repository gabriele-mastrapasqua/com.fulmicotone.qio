package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.abstracts;


import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.interfaces.IExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.interfaces.IStreamShardHelper;

import java.util.Set;

public abstract class AbstractStreamShardHelper implements IStreamShardHelper {

    protected String streamName;
    protected Set<IExplicitShardKeyHelper> helpers;

    public AbstractStreamShardHelper(){
    }

    @Override
    public IStreamShardHelper withHelpers(Set<IExplicitShardKeyHelper> helpers) {
        this.helpers = helpers;
        return this;
    }

    @Override
    public IStreamShardHelper withStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public String getStreamName() {
        return streamName;
    }

}
