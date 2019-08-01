package com.fulmicotone.qio.utils.kinesis.v2.streams.consumer;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.services.QueueIOService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * This is a pure wrapper to KclWorker from AWS, using QIO services could be an overhead because KclWorker is single-thread by design.
 * We use the KinesisRecordProcessor to convert the ByteBuffer datas we receiving from Kinesis indo Java objects and push them to the right queue.
 *
*/
public class KinesisConsumerQIOService extends QueueIOService<Void, Void> {

    private ShardRecordProcessorFactory recordProcessorFactory;
    private Scheduler scheduler;
    private ConfigsBuilder configsBuilder;
    private String streamName;
    private String applicationName;
    private Region region = Region.US_EAST_1;
    private int maxConcurrency = 100;

    public KinesisConsumerQIOService(ShardRecordProcessorFactory recordProcessorFactory, String streamName, String applicationName, Region region, int maxConcurrency) {
        super(Void.class, 1, 1, null, t -> t);
        this.recordProcessorFactory = recordProcessorFactory;
        this.streamName=streamName;
        this.applicationName=applicationName;
        this.region=region;
        this.maxConcurrency = maxConcurrency;
        startKCL();
    }


    public void startKCL()
    {
        // create async v2 client using KinesisClientUtil - see: https://docs.amazonaws.cn/en_us/streams/latest/dev/kcl-migration.html#worker-migration
        // as suggested by documentation, set max concurrency high enough to the correct usage of the client
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                /*.httpClientBuilder(NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(this.maxConcurrency))*/
                .region(region)
        );
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();

        this.configsBuilder = new ConfigsBuilder(streamName, applicationName, kinesisClient, dynamoClient, cloudWatchClient,
                UUID.randomUUID().toString(), this.recordProcessorFactory);

        this.scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );
    }

    @Override
    public void startConsuming() {
        singleExecutor = initSingleThreadExecutor();
        multiThreadExecutor = initSingleThreadExecutor();
        singleExecutor.execute(scheduler);
    }

    public boolean stopKCL(){
        try {
            scheduler.startGracefulShutdown().get(60, TimeUnit.SECONDS);
            scheduler = null;
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean isRunning() {
        return isKCLRunning();
    }

    public boolean isKCLRunning(){
        return scheduler != null;
    }

    @Override
    public IQueueIOIngestionTask<Void> ingestionTask() {
        return null;
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        scheduler.shutdown();
    }
}
