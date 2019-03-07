package com.fulmicotone.qio.factories;


import com.fulmicotone.qio.interfaces.IQueueIOService;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;



public class QueueIOServiceFactory {

    private Map<Class, Set<IQueueIOService<?>>> serviceMap = new HashMap<>();



    public <E>void registerObject(IQueueIOService<E> obj) {

        Set<IQueueIOService<?>> set = serviceMap.getOrDefault(obj.getInputClass(), new HashSet<>());
        set.add(obj);
        serviceMap.putIfAbsent(obj.getInputClass(), set);
    }

    public <E>void putObjectInQueues(E obj) {

        getMapSet(obj.getClass())
                .ifPresent(l -> l.forEach(queue -> {
                    BlockingQueue<E> q = (BlockingQueue<E>) queue;
                    q.add(obj);
                }));
    }

    public <E>void putObjectsInQueues(List<E> obj) {

        getMapSet(obj.getClass())
                .ifPresent(l -> l.forEach(queue -> {
                    BlockingQueue<E> q = (BlockingQueue<E>) queue;
                    q.addAll(obj);
                }));
    }

    private Optional<Set<BlockingQueue<?>>> getMapSet(Class clazz)
    {
        Set<IQueueIOService<?>> services = serviceMap.get(clazz);

        if(services == null)
            return Optional.empty();

        return Optional.of(services.stream().map(IQueueIOService::getInputQueue).collect(Collectors.toSet()));
    }
}
