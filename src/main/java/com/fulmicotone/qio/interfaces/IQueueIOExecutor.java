package com.fulmicotone.qio.interfaces;

import java.util.concurrent.Future;

public interface IQueueIOExecutor {

    int getActiveCount();
    int getMaximumPoolSize();
    int getQueueSize();
    Future<?> exec(IQueueIOExecutorTask r);
    void execute(Runnable r);
}
