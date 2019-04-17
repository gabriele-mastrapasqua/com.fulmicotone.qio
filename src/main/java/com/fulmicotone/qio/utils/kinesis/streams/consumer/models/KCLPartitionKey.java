package com.fulmicotone.qio.utils.kinesis.streams.consumer.models;

/**
 * Created by enryold on 20/12/16.
 */
public class KCLPartitionKey
{
    private String partitionKey;

    public KCLPartitionKey(String partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    public boolean equals(Object o)
    {
        return ((KCLPartitionKey)o).getPartitionKey().equals(this.getPartitionKey());
    }

    @Override
    public int hashCode() {
        return partitionKey.hashCode();
    }

    public String getPartitionKey()
    {
        return partitionKey;
    }


}
