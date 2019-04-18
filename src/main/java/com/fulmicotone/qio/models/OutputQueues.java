package com.fulmicotone.qio.models;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;

public class OutputQueues {

    private HashMap<Class<?>, List<BlockingQueue<?>>> queueMap = new HashMap<>();


    public OutputQueues withQueue(Class<?> clazz, BlockingQueue<?> queue)
    {
        List<BlockingQueue<?>> queueList = queueMap.getOrDefault(clazz, new ArrayList<>());
        queueList.add(queue);
        queueMap.put(clazz, queueList);
        return this;
    }

    public <I>Optional<List<TransferQueue<I>>> getQueues(Class<I> clazz)
    {
        if(queueMap.get(clazz) == null)
            return Optional.empty();

        List<TransferQueue<I>>  queues = queueMap
                .get(clazz)
                .stream()
                .map(o ->(TransferQueue<I>)o)
                .collect(Collectors.toList());

        if(queues == null || queues.size() == 0)
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
