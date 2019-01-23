package com.fulmicotone.qio.executors;

import co.paralleluniverse.common.monitoring.MonitorType;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberExecutorScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableCallable;
import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorTask;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;


import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class QueueIOFiberExecutor extends FiberExecutorScheduler implements IQueueIOExecutor {


    public QueueIOFiberExecutor(String name, ThreadPoolExecutor executor, MonitorType monitorType, boolean detailedInfo) {
        super(name, executor, monitorType, detailedInfo);
    }

    public QueueIOFiberExecutor(String name, ThreadPoolExecutor executor) {
        super(name, executor);
    }

    @Override
    public int getQueueLength() {
        return super.getQueueLength();
    }

    public int getActiveCount() {
        return ((ThreadPoolExecutor)super.getExecutor()).getActiveCount();
    }

    public int getPoolSize() {
        return ((ThreadPoolExecutor)super.getExecutor()).getPoolSize();
    }

    public int getMaximumPoolSize() {
        return ((ThreadPoolExecutor)super.getExecutor()).getMaximumPoolSize();
    }

    public boolean isTerminated() {
        return ((ThreadPoolExecutor)super.getExecutor()).isTerminated();
    }

    public BlockingQueue getQueue()
    {
        return ((ThreadPoolExecutor)super.getExecutor()).getQueue();
    }

    public void exec(IQueueIOExecutorTask task)
    {
        new Fiber<>(this, (SuspendableCallable<Void>) task::run).start();
    }

    @Override
    public <I> void exec(IQueueIOIngestionTask<I> r, List<I> elms) {
        new Fiber<>(this, (SuspendableCallable<Void>) () -> r.ingest(elms)).start();
    }
}
