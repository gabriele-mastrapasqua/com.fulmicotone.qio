package com.fulmicotone.qio.executors;



import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorTask;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;

import java.util.List;
import java.util.concurrent.*;

public class QueueIOExecutor extends ThreadPoolExecutor implements IQueueIOExecutor {


    public QueueIOExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public QueueIOExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public QueueIOExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public QueueIOExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    public int getQueueLength() {
        return super.getQueue().size();
    }

    @Override
    public void exec(IQueueIOExecutorTask r) {
        submit((Runnable) r::run);
    }
    public <I>void exec(IQueueIOIngestionTask<I> r, List<I> elms) {
        submit(() -> {
            r.ingest(elms);
        });
    }

}
