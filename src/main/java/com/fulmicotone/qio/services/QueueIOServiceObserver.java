package com.fulmicotone.qio.services;

import com.fulmicotone.qio.interfaces.IQueueIOService;
import com.fulmicotone.qio.models.QueueIOQ;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class QueueIOServiceObserver {

    private QueueIOStatusService statusService;
    private Map<Class, Set<IQueueIOService<?, ?>>> serviceMap = new ConcurrentHashMap<>();


    public QueueIOServiceObserver withStatusService(QueueIOStatusService statusService){
        this.statusService = statusService;
        return this;
    }

    public <E> void registerObject(QueueIOService<E, ?> obj) {
        Optional.ofNullable(this.statusService).ifPresent((a) -> {
            a.registerQueueIOService(obj);
        });
        Set<IQueueIOService<?, ?>> set = (Set)this.serviceMap.getOrDefault(obj.getInputClass(), new HashSet());
        set.add(obj);
        this.serviceMap.putIfAbsent(obj.getInputClass(), set);
    }


    public <E>void sendBroadcast(E obj) {

        getMapSet(obj.getClass())
                .ifPresent(l -> l.forEach(queue -> {
                    QueueIOQ<E> q = (QueueIOQ<E>) queue;
                    q.add(obj);
                }));
    }

    public <E>void sendBroadcast(List<E> obj) {

        getMapSet(obj.getClass())
                .ifPresent(l -> l.forEach(queue -> {
                    QueueIOQ<E> q = (QueueIOQ<E>) queue;
                    q.addAll(obj);
                }));
    }

    private Optional<Set<QueueIOQ<?>>> getMapSet(Class clazz)
    {
        Set<IQueueIOService<?, ?>> services = serviceMap.get(clazz);

        if(services == null)
            return Optional.empty();

        return Optional.of(services.stream().map(IQueueIOService::getInputQueue).collect(Collectors.toSet()));
    }
}
