package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.factories;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.KCLRoundRobinExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.abstracts.AbstractHashProviderFactory;


public class KCLRoundRobinExplicitHashProviderFactory extends AbstractHashProviderFactory<KCLRoundRobinExplicitHashProvider> {

    public KCLRoundRobinExplicitHashProviderFactory(AmazonKinesis amazonKinesis) {
        super(amazonKinesis);
    }

    public KCLRoundRobinExplicitHashProviderFactory(AmazonKinesis amazonKinesis, boolean refreshData) {
        super(amazonKinesis, refreshData);
    }

    @Override
    public KCLRoundRobinExplicitHashProvider getHashProvider() {
        return new KCLRoundRobinExplicitHashProvider(shardKeyHelper);
    }
}
