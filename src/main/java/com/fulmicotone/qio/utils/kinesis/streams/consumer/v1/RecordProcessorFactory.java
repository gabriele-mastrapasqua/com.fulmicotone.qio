package com.fulmicotone.qio.utils.kinesis.streams.consumer.v1;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.Record;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.models.KCLConsumerEntry;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Created by enryold on 20/12/16.
 */

public class RecordProcessorFactory implements IRecordProcessorFactory {


    private int backoffTime;
    private int retriesNumber;
    private int checkpointInterval;
    private ThreadPoolExecutor threadPoolExecutor;
    private Set<KCLConsumerEntry> possibleOutputs;
    private Consumer<Record> recordConsumer;


    public RecordProcessorFactory withPossibleOutputs(Set<KCLConsumerEntry> map)
    {
        this.possibleOutputs = map;
        return this;
    }

    public RecordProcessorFactory withBackoffTime(int backoffTime)
    {
        this.backoffTime = backoffTime;
        return this;
    }

    public RecordProcessorFactory withRetriesNumber(int retriesNumber)
    {
        this.retriesNumber = retriesNumber;
        return this;
    }

    public RecordProcessorFactory withCheckpointInterval(int checkpointInterval)
    {
        this.checkpointInterval = checkpointInterval;
        return this;
    }

    public RecordProcessorFactory withExecutor(ThreadPoolExecutor threadPoolExecutor)
    {
        this.threadPoolExecutor = threadPoolExecutor;
        return this;
    }

    public RecordProcessorFactory withProcessRecordCallback(Consumer<Record> recordCallback)
    {
        this.recordConsumer = recordCallback;
        return this;
    }


    @Override
    public IRecordProcessor createProcessor()
    {
        return new RecordProcessor()
                .withPossibileOutputs(new HashSet<>(possibleOutputs))
                .withBackoffTime(backoffTime)
                .withCheckpointInterval(checkpointInterval)
                .withExecutor(threadPoolExecutor)
                .withProcessRecordCallback(recordConsumer)
                .withRetriesNumber(retriesNumber);
    }
}
