package com.fulmicotone.qio.utils.kinesis.v2.streams.consumer;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.services.QueueIOService;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
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

    public KinesisConsumerQIOService(ConfigsBuilder configsBuilder, Scheduler scheduler) {
        super(Void.class, 1, 1, null, t -> t);
        this.configsBuilder = configsBuilder;
        this.scheduler=scheduler;
        //startKCL();
    }


    /*public void startKCL()
    {
        this.scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );
    }*/

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
