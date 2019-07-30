package com.fulmicotone.qio.utils.kinesis.v2.streams.consumer.v2;

import com.fulmicotone.qio.utils.kinesis.v2.streams.consumer.v2.models.KCLConsumer;
import com.fulmicotone.qio.utils.kinesis.v2.streams.consumer.v2.models.KCLConsumerEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Created by enryold on 20/12/16.
 */
public class RecordProcessor implements ShardRecordProcessor {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private int backoffTime;
    private int retriesNumber;
    private int checkpointInterval;
    private ThreadPoolExecutor threadPoolExecutor;
    private Consumer<KinesisClientRecord> recordConsumer;


    private String shardId;
    private long nextCheckpointTimeInMillis;
    private Map<String, List<KCLConsumer>> possibileOutputs = new HashMap<>();


    public RecordProcessor withPossibileOutputs(Set<KCLConsumerEntry> outputs)
    {
        outputs.forEach(entry -> possibileOutputs.put(entry.getConsumerKey().getPartitionKey(), entry.getConsumerFunctions()));
        return this;
    }

    public RecordProcessor withBackoffTime(int backoffTime)
    {
        this.backoffTime = backoffTime;
        return this;
    }

    public RecordProcessor withRetriesNumber(int retriesNumber)
    {
        this.retriesNumber = retriesNumber;
        return this;
    }

    public RecordProcessor withCheckpointInterval(int checkpointInterval)
    {
        this.checkpointInterval = checkpointInterval;
        return this;
    }

    public RecordProcessor withExecutor(ThreadPoolExecutor threadPoolExecutor)
    {
        this.threadPoolExecutor = threadPoolExecutor;
        return this;
    }

    public RecordProcessor withProcessRecordCallback(Consumer<KinesisClientRecord> recordCallback)
    {
        this.recordConsumer = recordCallback;
        return this;
    }


    @Override
    public void initialize(InitializationInput initializationInput) {
        String shardId = initializationInput.shardId();
        log.debug("Initializing with shard: "+shardId);
        this.shardId = shardId;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {

        List<KinesisClientRecord> records = processRecordsInput.records();
        RecordProcessorCheckpointer checkpointer = processRecordsInput.checkpointer();


        log.debug("Processing " + records.size() + " records from " + shardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointInterval;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<KinesisClientRecord> records) {
        for (KinesisClientRecord record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < retriesNumber; i++) {
                try
                {
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    log.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(backoffTime);
                } catch (InterruptedException e) {
                    log.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                log.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(KinesisClientRecord record) {

        List<KCLConsumer> list = this.possibileOutputs.get(record.partitionKey());

        if(list == null)
            return;

        log.debug("Received new object from partition key: "+record.partitionKey()+" with sequence number: "+record.sequenceNumber());

        if(threadPoolExecutor == null)
            process(record, list);
        else
            threadPoolExecutor.execute(() -> process(record, list));

    }


    private void process(KinesisClientRecord record, List<KCLConsumer> list)
    {
        list.forEach(e -> {
            if(recordConsumer != null) { recordConsumer.accept(record); }
            e.putInQueue(record.data());
                    log.debug("Object " + record.sequenceNumber() + " with class ["+e.getClassName()+"] send to ["+e.getFriendlyName()+"] queue successfully!");
                }
        );
    }



    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {

    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.debug("Shard ended down record processor for shard: " + shardId);
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.debug("Shutting down record processor for shard: " + shardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }


    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing shard " + shardId);
        for (int i = 0; i < retriesNumber; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                log.debug("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (retriesNumber - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    log.debug("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + retriesNumber, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(backoffTime);
            } catch (InterruptedException e) {
                log.debug("Interrupted sleep", e);
            }
        }
    }
}
