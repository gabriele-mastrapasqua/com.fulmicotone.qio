package com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.models;

import java.util.List;

/**
 * Created by enryold on 20/12/16.
 */
public class KCLConsumerEntry<I>
{
    private KCLPartitionKey consumerKey;
    private List<KCLConsumer<I, ?>> consumerFunctions;


    public KCLConsumerEntry(KCLPartitionKey consumerKey, List<KCLConsumer<I, ?>> consumerFunctions)
    {
        this.consumerFunctions = consumerFunctions;
        this.consumerKey = consumerKey;
    }

    public KCLPartitionKey getConsumerKey() {
        return consumerKey;
    }

    public List<KCLConsumer<I, ?>> getConsumerFunctions() {
        return consumerFunctions;
    }


    @Override
    public int hashCode() {
        return consumerKey.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this.consumerKey.equals(((KCLConsumerEntry)obj).getConsumerKey());
    }
}
