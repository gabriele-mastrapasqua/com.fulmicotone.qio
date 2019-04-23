package com.fulmicotone.qio.interfaces;

public interface IQueueIOExecutorFactory {

    IQueueIOExecutor createExecutor(String name, int nThreads, int capacity);
}
