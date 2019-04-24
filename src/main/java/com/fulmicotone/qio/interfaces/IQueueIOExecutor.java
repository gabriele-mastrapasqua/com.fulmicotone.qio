package com.fulmicotone.qio.interfaces;

public interface IQueueIOExecutor {

    int getActiveCount();
    int getMaximumPoolSize();
    int getQueueSize();
    void exec(IQueueIOExecutorTask r);
    void execute(Runnable r);
}
