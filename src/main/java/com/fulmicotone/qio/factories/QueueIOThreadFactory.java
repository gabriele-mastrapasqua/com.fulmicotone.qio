package com.fulmicotone.qio.factories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


public class QueueIOThreadFactory implements ThreadFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());


    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final Consumer<Thread> onThreadCreationCallback;

    public QueueIOThreadFactory(String name, Consumer<Thread> threadCreationCallback) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        namePrefix = "qio-" + name + "-" +
                poolNumber.getAndIncrement() +
                "-t-";
        onThreadCreationCallback = threadCreationCallback;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                namePrefix+threadNumber.getAndIncrement(),
                0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);

        t.setUncaughtExceptionHandler((t1, e) -> {
            log.error("UncaughtException in thread: "+ t1.getName()+" - message: "+e.getMessage());
            e.printStackTrace();
        });


        if(onThreadCreationCallback != null)
            onThreadCreationCallback.accept(t);

        return t;
    }

}
