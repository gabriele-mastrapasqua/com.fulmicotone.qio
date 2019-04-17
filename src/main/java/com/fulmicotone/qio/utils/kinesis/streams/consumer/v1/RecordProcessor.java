package com.fulmicotone.qio.utils.kinesis.streams.consumer.v1;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.models.KCLConsumer;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.models.KCLConsumerEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Created by enryold on 20/12/16.
 */
public class RecordProcessor implements IRecordProcessor {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private int backoffTime;
    private int retriesNumber;
    private int checkpointInterval;
    private ThreadPoolExecutor threadPoolExecutor;
    private Consumer<Record> recordConsumer;


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

    public RecordProcessor withProcessRecordCallback(Consumer<Record> recordCallback)
    {
        this.recordConsumer = recordCallback;
        return this;
    }


    @Override
    public void initialize(String shardId) {

        log.debug("Initializing with shard: "+shardId);
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
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
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
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
    private void processSingleRecord(Record record) {

        List<KCLConsumer> list = this.possibileOutputs.get(record.getPartitionKey());

        if(list == null)
            return;

        log.debug("Received new object from partition key: "+record.getPartitionKey()+" with sequence number: "+record.getSequenceNumber());

        if(threadPoolExecutor == null)
            process(record, list);
        else
            threadPoolExecutor.execute(() -> process(record, list));

    }


    private void process(Record record, List<KCLConsumer> list)
    {
        list.forEach(e -> {
            if(recordConsumer != null) { recordConsumer.accept(record); }
            e.putInQueue(record.getData());
                    log.debug("Object " + record.getSequenceNumber() + " with class ["+e.getClassName()+"] send to ["+e.getFriendlyName()+"] queue successfully!");
                }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        log.debug("Shutting down record processor for shard: " + shardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
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
