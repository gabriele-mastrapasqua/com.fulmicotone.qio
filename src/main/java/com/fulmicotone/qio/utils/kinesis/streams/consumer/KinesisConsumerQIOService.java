package com.fulmicotone.qio.utils.kinesis.streams.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.services.QueueIOService;


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
        init();
    }


    private void init()
    {
        this.kclWorker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kinesisClientLibConfiguration)
                .build();
    }

    @Override
    public void startConsuming() {
        this.singleExecutor.execute(kclWorker);
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
