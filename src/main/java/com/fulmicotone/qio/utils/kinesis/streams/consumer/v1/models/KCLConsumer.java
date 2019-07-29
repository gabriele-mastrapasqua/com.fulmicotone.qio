package com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.models;


import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisDataTransform;
import com.fulmicotone.qio.utils.kinesis.streams.common.interfaces.IKinesisListDecoder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Created by enryold on 20/12/16.
 */
public class KCLConsumer<T, O>
{
    private IKinesisListDecoder<T> decoderList;
    private Queue destinationQueue;
    private Class<T> clazz;
    private IKinesisDataTransform<T, ?> transform;
    private String friendlyName;



    public KCLConsumer(IKinesisListDecoder<T> decoder, Queue<T> destinationQueue, Class<T> clazz, String friendlyName)
    {
        this.decoderList = decoder;
        this.destinationQueue = destinationQueue;
        this.clazz = clazz;
        this.friendlyName = friendlyName;
        this.transform = (IKinesisDataTransform<T, T>) tOpt ->  tOpt.map(Collections::singletonList);
    }

    public KCLConsumer(IKinesisListDecoder<T> decoder, IKinesisDataTransform<T, O> transform, Queue<O> destinationQueue, Class<T> clazz, String friendlyName)
    {
        this.decoderList = decoder;
        this.destinationQueue = destinationQueue;
        this.clazz = clazz;
        this.friendlyName = friendlyName;
        this.transform = transform;
    }

    public String getClassName()
    {
        return clazz.getSimpleName();
    }
    public String getFriendlyName()
    {
        return friendlyName;
    }

    public void putInQueue(ByteBuffer o)
    {


        if(decoderList != null)
        {
            List<T> res = decoderList.apply(o, clazz);

            if(res == null){ return; }

            res.stream()
                    .map(el -> transform.apply(Optional.of(el)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(destinationQueue::addAll);
        }

    }



}
