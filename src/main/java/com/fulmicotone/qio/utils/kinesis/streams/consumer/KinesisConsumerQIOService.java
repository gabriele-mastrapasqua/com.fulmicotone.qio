package com.fulmicotone.qio.utils.kinesis.streams.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.services.QueueIOService;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * This is a pure wrapper to KclWorker from AWS, using QIO services could be an overhead because KclWorker is single-thread by design.
 * We use the KinesisRecordProcessor to convert the ByteBuffer datas we receiving from Kinesis indo Java objects and push them to the right queue.
 *
*/
public class KinesisConsumerQIOService extends QueueIOService<Void, Void> {

    private IRecordProcessorFactory recordProcessorFactory;
    private KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private Worker kclWorker;

    public KinesisConsumerQIOService(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration kinesisClientLibConfiguration) {
        super(Void.class, 1, 1, null, t -> t);
        this.recordProcessorFactory = recordProcessorFactory;
        this.kinesisClientLibConfiguration = kinesisClientLibConfiguration;
        startKCL();
    }


    public void startKCL()
    {
        this.kclWorker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kinesisClientLibConfiguration)
                .build();
    }

    @Override
    public void startConsuming() {
        singleExecutor = initSingleThreadExecutor();
        multiThreadExecutor = initSingleThreadExecutor();
        singleExecutor.execute(kclWorker);
    }

    private boolean stopKCL(){
        try {
            kclWorker.startGracefulShutdown().get(60, TimeUnit.SECONDS);
            kclWorker = null;
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean isKCLRunning(){
        return kclWorker == null;
    }

    @Override
    public IQueueIOIngestionTask<Void> ingestionTask() {
        return null;
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        kclWorker.shutdown();
    }
}
