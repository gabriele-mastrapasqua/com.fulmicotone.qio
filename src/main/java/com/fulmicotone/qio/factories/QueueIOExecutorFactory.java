package com.fulmicotone.qio.factories;



import com.fulmicotone.qio.executors.QueueIOExecutor;
import com.fulmicotone.qio.executors.QueueIOFiberExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public class QueueIOExecutorFactory {


    public static IQueueIOExecutor createExecutor(String name, int nThreads, int capacity)
    {

        return createExecutor(name,nThreads, capacity,null);
    }

    public static IQueueIOExecutor createExecutor(String name,int nThreads, int capacity, Consumer<Thread> onThreadCreationCallback)
    {

        return new QueueIOExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                new QueueIOThreadFactory(name.toLowerCase(), onThreadCreationCallback),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static IQueueIOExecutor createFiberExecutor(String name,int nThreads, int capacity)
    {
        return createFiberExecutor(name,nThreads, capacity,null);
    }

    public static IQueueIOExecutor createFiberExecutor(String name,int nThreads, int capacity, Consumer<Thread> onThreadCreationCallback)
    {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                new QueueIOThreadFactory(name.toLowerCase(), onThreadCreationCallback),
                new ThreadPoolExecutor.AbortPolicy());

        return new QueueIOFiberExecutor("Fiber"+name, executor);
    }


}
