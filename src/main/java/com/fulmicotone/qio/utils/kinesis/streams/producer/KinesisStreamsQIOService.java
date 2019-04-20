package com.fulmicotone.qio.utils.kinesis.streams.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOService;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IExplicitHashProviderFactory;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

public class KinesisStreamsQIOService<I> extends QueueIOService<I, ByteBuffer> {

    final Logger log = LoggerFactory.getLogger(this.getClass());
    private String streamName;
    private String partitionKey;
    private IExplicitHashProviderFactory explicitHashProviderFactory;
    private KinesisProducer kinesisProducer;
    private Consumer<ListenableFuture<UserRecordResult>> userRecordConsumer;


    public KinesisStreamsQIOService(Class<I> clazz, Integer threadSize) {
        super(clazz, threadSize, null, null);
    }

    public KinesisStreamsQIOService(Class<I> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<I, ByteBuffer> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public KinesisStreamsQIOService(Class<I> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<I, ByteBuffer> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
    }

    public KinesisStreamsQIOService<I> withStreamName(String streamName){
        this.streamName = streamName;
        return this;
    }

    public KinesisStreamsQIOService<I> withPartitionKey(String partitionKey){
        this.partitionKey = partitionKey;
        return this;
    }

    public KinesisStreamsQIOService<I> withKinesisProducer(KinesisProducer kinesisProducer){
        this.kinesisProducer = kinesisProducer;
        return this;
    }

    public KinesisStreamsQIOService<I> withExplicitHashProviderFactory(IExplicitHashProviderFactory explicitHashProviderFactory){
        this.explicitHashProviderFactory = explicitHashProviderFactory;
        return this;
    }

    public KinesisStreamsQIOService<I> withUserRecordConsumer(Consumer<ListenableFuture<UserRecordResult>> userRecordConsumer){
        this.userRecordConsumer = userRecordConsumer;
        return this;
    }




    @Override
    public IQueueIOIngestionTask<ByteBuffer> ingestionTask() {

        if(explicitHashProviderFactory == null){
            return new KinesisStreamsIngestionTask<ByteBuffer>() {

                @Override
                public Void ingest(List<ByteBuffer> list) {

                    try {
                        list.forEach(r -> putRecord(r));
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    return null;



                }
            };
        }
        else{
            return new KinesisStreamsIngestionTask<ByteBuffer>(explicitHashProviderFactory.getHashProvider().nextHashKey()) {

                @Override
                public Void ingest(List<ByteBuffer> list) {
                    try {
                        list.forEach(r -> putRecord(r, this.explicitHash));
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }


    }


    protected void putRecord(ByteBuffer record, String explicitHashKey) {



        ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(streamName, partitionKey, explicitHashKey, record);

        if(userRecordConsumer != null){
            userRecordConsumer.accept(future);
        }

        producedObjectNotification(record);
        producedBytesNotification(record.array());
    }

    protected void putRecord(ByteBuffer record) {

        ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(streamName, partitionKey, record);

        if(userRecordConsumer != null){
            userRecordConsumer.accept(future);
        }

        producedObjectNotification(record);
        producedBytesNotification(record.array());

    }




}
