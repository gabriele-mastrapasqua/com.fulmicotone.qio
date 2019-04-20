package com.fulmicotone.qio.models;


import java.util.*;
import java.util.stream.Collectors;

public class OutputQueues {

    private HashMap<Class<?>, List<Queue<?>>> queueMap = new HashMap<>();


    public OutputQueues withQueue(Class<?> clazz, Queue<?> queue)
    {
        List<Queue<?>> queueList = queueMap.getOrDefault(clazz, new ArrayList<>());
        queueList.add(queue);
        queueMap.put(clazz, queueList);
        return this;
    }

    public <I>Optional<List<Queue<I>>> getQueues(Class<I> clazz)
    {
        if(queueMap.get(clazz) == null)
            return Optional.empty();

        List<Queue<I>>  queues = queueMap
                .get(clazz)
                .stream()
                .map(o ->(Queue<I>)o)
                .collect(Collectors.toList());

        if(queues.size() == 0)
            return Optional.empty();
        else
            return Optional.of(queues);
    }

    public <I>void pushInQueue(Class<I> clazz, I elm)
    {
        getQueues(clazz).ifPresent(queues -> queues.forEach(q -> q.add(elm)));
    }

    public <I>void pushAllInQueue(Class<I> clazz, List<I> elms)
    {
        getQueues(clazz).ifPresent(queues -> queues.forEach(q -> q.addAll(elms)));
    }

}
