package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.abstracts;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.listener.RetryListener;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.KCLShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitHashProvider;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitHashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.utils.ShardHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public abstract class AbstractHashProviderFactory<I extends IExplicitHashProvider> implements IExplicitHashProviderFactory<I> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());


    private AmazonKinesis amazonKinesis;
    private String streamName;
    protected ShardHelper shardKeyHelper = new ShardHelper();


    public AbstractHashProviderFactory(AmazonKinesis amazonKinesis, String streamName) {
        this.amazonKinesis = amazonKinesis;
        this.streamName = streamName;
        this.init();
    }


    private void init() {
        log.info("KCL Describe stream "+streamName+" init!");

        Callable<ShardHelper> callable = () -> {

            ShardHelper copy = new ShardHelper();

            DescribeStreamResult response = amazonKinesis.describeStream(streamName);

            copy.putShardKeyHelpers(streamName, response.getStreamDescription()
                    .getShards()
                    .stream()
                    .peek(s -> log.info("Shard range for "+streamName+": "+s.getHashKeyRange()))
                    .map(s -> new KCLShardKeyHelper(s.getShardId(), s.getHashKeyRange()))
                    .collect(Collectors.toSet()));

            return copy;
        };

        RetryConfig config = new RetryConfigBuilder()
                .exponentialBackoff5Tries5Sec()
                .build();

        CallExecutor<ShardHelper> executor = new CallExecutorBuilder<>()
                .config(config)
                .onSuccessListener(status -> log.info("Describe stream "+streamName+" success!"))
                .onFailureListener(status -> log.info("Describe stream "+streamName+" failed!"))
                .build();

        this.shardKeyHelper = executor.execute(callable).getResult();
    }

}
