package com.fulmicotone.qio.factories;


import com.fulmicotone.qio.executors.QueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class QueueIOExecutorFactory implements IQueueIOExecutorFactory {


    public IQueueIOExecutor createExecutor(String name, int nThreads, int capacity)
    {
        return new QueueIOExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                new QueueIOThreadFactory(name.toLowerCase(), null),
                new ThreadPoolExecutor.AbortPolicy());
    }




}
