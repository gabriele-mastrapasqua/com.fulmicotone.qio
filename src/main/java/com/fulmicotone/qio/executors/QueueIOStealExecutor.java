package com.fulmicotone.qio.executors;



import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorTask;

import java.util.concurrent.*;

public class QueueIOStealExecutor extends ForkJoinPool implements IQueueIOExecutor {


    public QueueIOStealExecutor(int parallelism,
                                ForkJoinWorkerThreadFactory factory,
                                Thread.UncaughtExceptionHandler handler,
                                boolean asyncMode) {
        super(parallelism, factory, handler, asyncMode);
    }


    @Override
    public int getActiveCount() {
        return this.getActiveThreadCount();
    }

    @Override
    public int getMaximumPoolSize() {
        return this.getPoolSize();
    }

    @Override
    public int getQueueSize() {
        return (int)this.getQueuedTaskCount();
    }


    @Override
    public void exec(IQueueIOExecutorTask r) {
        submit((Runnable) r::run);
    }


}
