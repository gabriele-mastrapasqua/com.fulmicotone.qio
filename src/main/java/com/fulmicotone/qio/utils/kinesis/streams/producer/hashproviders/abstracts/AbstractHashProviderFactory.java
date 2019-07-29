package com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.abstracts;

import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.ExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IExplicitHashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IStreamShardHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class AbstractHashProviderFactory implements IExplicitHashProviderFactory {

    private final Logger log = LoggerFactory.getLogger(this.getClass());


    private KinesisAsyncClient amazonKinesis;
    private String streamName;
    protected IStreamShardHelper shardKeyHelper;


    public AbstractHashProviderFactory(KinesisAsyncClient amazonKinesis, String streamName, IStreamShardHelper iStreamShardHelper) {
        this.amazonKinesis = amazonKinesis;
        this.streamName = streamName;
        this.shardKeyHelper = iStreamShardHelper;
        this.init();
    }


    protected void init() {
        log.info("KCL Describe stream "+streamName+" init!");


        CompletableFuture<Set<IExplicitShardKeyHelper>> setCompletableFuture = amazonKinesis.describeStream(builder -> builder.streamName(streamName))
                .thenApply(describeStreamResponse -> describeStreamResponse.streamDescription().shards()
                        .stream()
                        .peek(s -> log.info("Shard " + s.shardId() + " on " + streamName + ", range: " + s.hashKeyRange()))
                        .map(s -> new ExplicitShardKeyHelper(s.shardId(), s.hashKeyRange()))
                        .collect(Collectors.toSet()));


        RetryConfig config = new RetryConfigBuilder()
                .exponentialBackoff5Tries5Sec()
                .build();

        CallExecutor<Set<IExplicitShardKeyHelper>> executor = new CallExecutorBuilder<>()
                .config(config)
                .onSuccessListener(status -> log.info("Describe stream "+streamName+" success!"))
                .onFailureListener(status -> log.info("Describe stream "+streamName+" failed!"))
                .build();


        // convert to executor -> callable -> completable async future join
        Set<IExplicitShardKeyHelper> shardHelpers = executor.execute(
                (Callable<Set<IExplicitShardKeyHelper>>) () -> setCompletableFuture.join()
        ).getResult();


        shardKeyHelper.withHelpers(shardHelpers)
                .withStreamName(streamName);
    }

}
