package com.fulmicotone.qio.interfaces;

import java.util.List;
import java.util.concurrent.TransferQueue;

public interface IQueueIOService<I> {

    TransferQueue<I> getInputQueue();
    Class<I> getInputClass();
    String getUniqueKey();

    <I1>void producedObjectNotification(I1 object);
    <I1>void producedObjectsNotification(List<I1> object);
    void receivedObjectNotification(I object);

    void producedBytesNotification(byte[] object);
    void receivedBytesNotification(byte[] object);


    IQueueIOIngestionTask<I> ingestionTask();


    <E>void produce(E elm, Class<E> clazz);
    <E>void produceAll(List<E> elm, Class<E> clazz);

    void flush();
    void onDestroy();
}

