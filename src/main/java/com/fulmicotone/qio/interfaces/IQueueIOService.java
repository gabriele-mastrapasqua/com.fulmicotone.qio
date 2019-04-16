package com.fulmicotone.qio.interfaces;

import com.fulmicotone.qio.models.QueueIOQ;

import java.util.List;

// Input type I and optional T as (T)ransformed
public interface IQueueIOService<I, T> {

    QueueIOQ<I> getInputQueue();
    int getInternalThreads();
    Class<I> getInputClass();
    String getUniqueKey();

    <I1>void producedObjectNotification(I1 object);
    <I1>void producedObjectsNotification(List<I1> object);
    void receivedObjectNotification(I object);

    void producedBytesNotification(byte[] object);
    void receivedBytesNotification(byte[] object);


    IQueueIOIngestionTask<T> ingestionTask();


    <E>void produce(E elm, Class<E> clazz);
    <E>void produceAll(List<E> elm, Class<E> clazz);

    void updateMetrics();
    void registerMetrics(String appNamespace);
    void flush();
    void onDestroy();
}

