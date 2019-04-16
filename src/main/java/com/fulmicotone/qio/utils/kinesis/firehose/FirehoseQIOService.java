package com.fulmicotone.qio.utils.kinesis.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOService;
import com.fulmicotone.qio.utils.kinesis.firehose.enums.PutRecordMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FirehoseQIOService<I> extends QueueIOService<I, Record> {

    final Logger log = LoggerFactory.getLogger(this.getClass());
    private List<String> streamNames;
    private PutRecordMode putRecordMode = PutRecordMode.BATCH;
    private AmazonKinesisFirehoseClient amazonKinesisFirehoseClient;
    private boolean logRequests = false;


    public FirehoseQIOService(Class<I> clazz, Integer threadSize) {
        super(clazz, threadSize, null, null);
    }

    public FirehoseQIOService(Class<I> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<I, Record> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public FirehoseQIOService(Class<I> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<I, Record> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
    }

    public FirehoseQIOService<I> withPutRecordMode(PutRecordMode putRecordMode){
        this.putRecordMode = putRecordMode;
        return this;
    }

    public FirehoseQIOService<I> withAmazonKinesisFirehoseClient(AmazonKinesisFirehoseClient amazonKinesisFirehoseClient){
        this.amazonKinesisFirehoseClient = amazonKinesisFirehoseClient;
        return this;
    }

    public FirehoseQIOService<I> withStreamNames(List<String> streamNames){
        this.streamNames = streamNames;
        return this;
    }


    private void sendRecords(List<Record> list, String streamName) {


        switch (putRecordMode)
        {
            case SINGLE: {
                list.forEach(c -> putRecord(c, streamName));
                break;
            }
            case BATCH:{
                putRecordBatch(list, streamName);
                break;
            }
        }

    }



    @Override
    public IQueueIOIngestionTask<Record> ingestionTask() {
        return new FirehoseIngestionTask<Record>(new ArrayList<>(streamNames)) {

            @Override
            public Void ingest(List<Record> list) {
                sendRecords(list, this.getStreamName());
                return null;
            }
        };
    }


    protected void putRecord(Record record, String streamName) {


        try
        {

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(streamName);
            putRecordRequest.setRecord(record);
            amazonKinesisFirehoseClient.putRecord(putRecordRequest);

            if(logRequests){
                log.debug("PutRecord in stream "+streamName+" with hash "+record.hashCode()+" and of "+record.getData().array().length+" bytes");
            }

            producedObjectNotification(record);
        }
        catch (Exception e)
        {
            log.error("PutRecord in stream "+streamName+" failed "+e.getMessage());
        }

    }

    protected void putRecordBatch(List<Record> list, String streamName) {


        try
        {

            if(list.size() == 1)
            {
                putRecord(list.get(0), streamName);
                return;
            }

            PutRecordBatchRequest putRecordRequest = new PutRecordBatchRequest();
            putRecordRequest.setDeliveryStreamName(streamName);
            putRecordRequest.setRecords(list);
            amazonKinesisFirehoseClient.putRecordBatch(putRecordRequest);

            if(logRequests) {
                log.debug("PutRecordBatch in stream "+streamName+" of "+list.size()+" records with hash "+list.hashCode()+" and size of "+list.stream().mapToInt(r -> r.getData().array().length).sum()+" bytes");
            }

            producedObjectsNotification(list);
        }
        catch (Exception e)
        {
            log.error("PutRecordBatch in stream "+streamName+" failed "+e.getMessage());
        }

    }



}
