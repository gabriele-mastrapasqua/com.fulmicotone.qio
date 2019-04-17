package com.fulmicotone.qio.utils.kinesis.streams;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class KinesisStreamsIngestionTask<I> implements IQueueIOIngestionTask<I> {

    protected String explicitHash;

    public KinesisStreamsIngestionTask(String explicitHash){
        this.explicitHash = explicitHash;
    }

    public KinesisStreamsIngestionTask(){
    }

}
