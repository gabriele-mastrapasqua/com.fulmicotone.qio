package com.fulmicotone.qio.utils.kinesis.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOService;
import com.fulmicotone.qio.utils.kinesis.firehose.enums.PutRecordMode;
import com.fulmicotone.qio.utils.kinesis.firehose.models.FirehoseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class FirehoseQIOService<I,T> extends QueueIOService<FirehoseWrapper<I>> {

    final Logger log = LoggerFactory.getLogger(this.getClass());
    private String streamName;
    private PutRecordMode putRecordMode;
    private AmazonKinesisFirehoseClient amazonKinesisFirehoseClient;
    private boolean logRequests = false;



    public FirehoseQIOService(Class<FirehoseWrapper<I>> clazz, Integer threadSize, OutputQueues outputQueues) {
        super(clazz, threadSize, outputQueues);
    }

    public FirehoseQIOService(Class<FirehoseWrapper<I>> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues);
    }


    @Override
    public IQueueIOIngestionTask<FirehoseWrapper<I>> ingestionTask() {
        return list -> {

            sendRecords(list.stream()
                    .flatMap(w -> w.getTransformedRecords().stream())
                    .collect(Collectors.toList()));

            return null;
        };
    }

    private void sendRecords(List<Record> list) {


        switch (putRecordMode)
        {
            case SINGLE: {
                list.forEach(this::putRecord);
                break;
            }
            case BATCH:{
                putRecordBatch(list);
                break;
            }
        }

    }


    private void putRecord(Record record) {


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

    private void putRecordBatch(List<Record> list) {


        try
        {

            if(list.size() == 1)
            {
                putRecord(list.get(0));
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
