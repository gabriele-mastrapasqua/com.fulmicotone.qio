package com.fulmicotone.qio.interfaces;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public interface IQueueIOExecutor {

    int getQueueLength();
    int getActiveCount();
    int getPoolSize();
    int getMaximumPoolSize();
    boolean isTerminated();
    BlockingQueue getQueue();
    void exec(IQueueIOExecutorTask r);
    <I>void exec(IQueueIOIngestionTask<I> r, List<I> elms);
    void execute(Runnable r);
}
