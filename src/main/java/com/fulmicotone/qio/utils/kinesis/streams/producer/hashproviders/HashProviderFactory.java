package com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.abstracts.AbstractHashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IStreamShardHelper;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;


public class HashProviderFactory extends AbstractHashProviderFactory {


    public HashProviderFactory(KinesisAsyncClient amazonKinesis, String streamName, IStreamShardHelper helper) {
        super(amazonKinesis, streamName, helper);
    }

    @Override
    public IExplicitHashProvider getHashProvider() {
        return new HashProvider(shardKeyHelper);
    }
}
