package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.abstracts;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.ExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitHashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IStreamShardHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public abstract class AbstractHashProviderFactory implements IExplicitHashProviderFactory {

    private final Logger log = LoggerFactory.getLogger(this.getClass());


    private AmazonKinesis amazonKinesis;
    private String streamName;
    protected IStreamShardHelper shardKeyHelper;


    public AbstractHashProviderFactory(AmazonKinesis amazonKinesis, String streamName, IStreamShardHelper iStreamShardHelper) {
        this.amazonKinesis = amazonKinesis;
        this.streamName = streamName;
        this.shardKeyHelper = iStreamShardHelper;
        this.init();
    }


    protected void init() {
        log.info("KCL Describe stream "+streamName+" init!");

        Callable<Set<IExplicitShardKeyHelper>> callable = () -> amazonKinesis.describeStream(streamName).getStreamDescription()
               .getShards()
               .stream()
               .peek(s -> log.info("Shard "+s.getShardId()+" on " + streamName + ", range: " + s.getHashKeyRange()))
               .map(s -> new ExplicitShardKeyHelper(s.getShardId(), s.getHashKeyRange()))
               .collect(Collectors.toSet());

        RetryConfig config = new RetryConfigBuilder()
                .exponentialBackoff5Tries5Sec()
                .build();

        CallExecutor<Set<IExplicitShardKeyHelper>> executor = new CallExecutorBuilder<>()
                .config(config)
                .onSuccessListener(status -> log.info("Describe stream "+streamName+" success!"))
                .onFailureListener(status -> log.info("Describe stream "+streamName+" failed!"))
                .build();

        Set<IExplicitShardKeyHelper> shardHelpers = executor.execute(callable).getResult();

        shardKeyHelper.withHelpers(shardHelpers)
                .withStreamName(streamName);
    }

}
