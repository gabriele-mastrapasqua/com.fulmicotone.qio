package com.fulmicotone.qio.interfaces;

import java.util.concurrent.BlockingQueue;

public interface IQueueIOExecutor {

    int getActiveCount();
    int getMaximumPoolSize();
    BlockingQueue getQueue();
    void exec(IQueueIOExecutorTask r);
    void execute(Runnable r);
}
