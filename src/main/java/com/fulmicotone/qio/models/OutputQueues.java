package com.fulmicotone.qio.models;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class OutputQueues {

    private Map<Class<?>, List<Queue<?>>> queueMap = new ConcurrentHashMap<>();


    public OutputQueues withQueue(Class<?> clazz, Queue<?> queue)
    {
        List<Queue<?>> queueList = queueMap.getOrDefault(clazz, new ArrayList<>());
        queueList.add(queue);
        queueMap.put(clazz, queueList);
        return this;
    }


    public boolean containsQueue(Class<?> clazz, Queue<?> queue)
    {
        if(queueMap.get(clazz) == null)
            return false;

        return queueMap
                .get(clazz)
                .stream()
                .filter(Objects::nonNull)
                .anyMatch(q -> q.hashCode() == queue.hashCode());
    }


    public <I>void pushInQueue(Class<I> clazz, I elm)
    {
        for(Queue<?> q : queueMap.get(clazz)){
            ((Queue<I>)q).add(elm);
        }
    }

    public <I>void pushAllInQueue(Class<I> clazz, List<I> elms)
    {
        for(Queue<?> q : queueMap.get(clazz)){
            ((Queue<I>)q).addAll(elms);
        }
    }

}
