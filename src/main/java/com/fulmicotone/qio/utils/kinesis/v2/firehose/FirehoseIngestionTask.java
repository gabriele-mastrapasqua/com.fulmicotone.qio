package com.fulmicotone.qio.utils.kinesis.v2.firehose;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FirehoseIngestionTask<I> implements IQueueIOIngestionTask<I> {

    private List<String> streamNames;
    private AtomicInteger streamIterator = new AtomicInteger(0);


    protected String getStreamName()
    {
        if(streamNames.size() == 0){
            return streamNames.get(0);
        }

        if(streamIterator.get() >= streamNames.size()){
            streamIterator.set(0);
        }
        return streamNames.get(streamIterator.getAndIncrement());
    }

    public FirehoseIngestionTask(List<String> streamNames){
        this.streamNames = streamNames;
    }

}
