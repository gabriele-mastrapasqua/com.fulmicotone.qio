package com.fulmicotone.qio.factories;


import com.fulmicotone.qio.executors.QueueIOStealExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorFactory;

import java.util.concurrent.ForkJoinPool;


public class QueueIOStealExecutorFactory implements IQueueIOExecutorFactory {


    public IQueueIOExecutor createExecutor(String name, int nThreads, int capacity)
    {
        return new QueueIOStealExecutor(nThreads,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                Thread.getDefaultUncaughtExceptionHandler(), true);
    }
}
