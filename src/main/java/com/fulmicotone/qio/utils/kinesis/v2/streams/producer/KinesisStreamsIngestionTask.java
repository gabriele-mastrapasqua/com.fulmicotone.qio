package com.fulmicotone.qio.utils.kinesis.v2.streams.producer;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;

public abstract class KinesisStreamsIngestionTask<I> implements IQueueIOIngestionTask<I> {

    protected String explicitHash;

    public KinesisStreamsIngestionTask(String explicitHash){
        this.explicitHash = explicitHash;
    }

    public KinesisStreamsIngestionTask(){
    }

}
