package com.fulmicotone.qio.models;



import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulator;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.components.metrics.QueueIOMetric;
import com.fulmicotone.qio.factories.QueueIOExecutorFactory;
import com.fulmicotone.qio.interfaces.IQueueIOExecutor;
import com.fulmicotone.qio.interfaces.IQueueIOExecutorTask;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOService;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public abstract class QueueIOService<E> implements IQueueIOService<E> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private Class<E> clazz;


    private QueueIOQ<E> inputQueue;
    private OutputQueues outputQueues;
    private IQueueIOExecutor multiThreadExecutor;
    private IQueueIOExecutor singleExecutor;
    private Map<Integer, QueueIOQ<E>> internalQueues = new HashMap<>();

    private QueueIOMetric queueIOMetric;
    private int internalQueueThreadCreationIndex = 0;
    private int maxInternalThreads;
    private int internalQueuesMaxSize = 10_000_000;
    private int multiThreadQueueSize;
    private int chunkSize = 100;
    private int flushTimeout = 30;
    private TimeUnit flushTimeUnit = TimeUnit.SECONDS;
    private boolean useQuasar;
    private boolean sizeBatchingEnabled = false;
    private boolean byteBatchingEnabled = false;
    private IQueueIOAccumulatorFactory<E> accumulatorFactory;


    // METRICS



    public QueueIOService(Class<E> clazz, Integer threadSize, OutputQueues outputQueues)
    {
        this(clazz, threadSize, 100_000_000, outputQueues);
    }

    public QueueIOService(Class<E> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues)
    {
        this.clazz = clazz;
        this.inputQueue = new QueueIOQ<>();
        this.outputQueues = outputQueues;
        this.maxInternalThreads = threadSize;
        this.multiThreadQueueSize = multiThreadQueueSize;
        this.queueIOMetric = new QueueIOMetric(this);
    }



    public <I extends QueueIOService<E>> I withQuasar(boolean withQuasar){
        this.useQuasar = withQuasar;
        return (I)this;
    }

    public <I extends QueueIOService<E>> I withQueueIOMetric(QueueIOMetric metric){
        this.queueIOMetric = metric;
        return (I)this;
    }

    public <I extends QueueIOService<E>> I withInternalQueuesMaxSize(int internalQueuesMaxSize){
        this.internalQueuesMaxSize = internalQueuesMaxSize;
        return (I)this;
    }

    public <I extends QueueIOService<E>> I withSizeBatchingPerConsumerThread(int chunkSize, int flushTimeout, TimeUnit flushTimeUnit){
        sizeBatchingEnabled = true;
        this.chunkSize = chunkSize;
        this.flushTimeout = flushTimeout;
        this.flushTimeUnit = flushTimeUnit;
        return (I)this;
    }

    public <I extends QueueIOService<E>> I withByteBatchingPerConsumerThread(IQueueIOAccumulatorFactory<E> accumulatorFactory, int flushTimeout, TimeUnit flushTimeUnit){
        byteBatchingEnabled = true;
        this.accumulatorFactory = accumulatorFactory;
        this.flushTimeout = flushTimeout;
        this.flushTimeUnit = flushTimeUnit;
        return (I)this;
    }

    @Override
    public QueueIOQ<E> getInputQueue() {
        return inputQueue;
    }

    @Override
    public Class<E> getInputClass() {
        return clazz;
    }

    @Override
    public String getUniqueKey() {
        return getClass().getSimpleName()+"<"+getInputClass().getSimpleName()+">";
    }

    private void initNewConsumerThread(Integer maxThreads)
    {

        IntStream.range(0, maxThreads).forEach(i -> {
            QueueIOQ<E> queue = internalQueues.get(getNextQueueToBindIndex());
            if(sizeBatchingEnabled){
                multiThreadExecutor.exec(buildInternalReceiverTaskSizeBatching(queue, ingestionTask(), chunkSize, flushTimeout, flushTimeUnit));
            }else if(byteBatchingEnabled){
                multiThreadExecutor.exec(buildInternalReceiverTaskByteBatching(queue, ingestionTask(), accumulatorFactory, flushTimeout, flushTimeUnit));
            }
            else{
                multiThreadExecutor.exec(buildInternalReceiverTask(queue, ingestionTask()));
            }
        });
    }

    private int getNextQueueToBindIndex(){
        if(internalQueueThreadCreationIndex >= maxInternalThreads){
            internalQueueThreadCreationIndex = 0 ;
        }
        return internalQueueThreadCreationIndex++;
    }


    protected IQueueIOExecutor initSingleThreadExecutor(){
        return QueueIOExecutorFactory.createExecutor(getClass().getSimpleName()+"-st",1, 1000000);
    }

    private IQueueIOExecutor initMultiThreadExecutor(Integer consumingThreads, Integer queueSize){

        if(useQuasar){
            return QueueIOExecutorFactory.createFiberExecutor(getClass().getSimpleName()+"-fmt", consumingThreads, queueSize);
        }else{
            return QueueIOExecutorFactory.createExecutor(getClass().getSimpleName()+"-mt", consumingThreads, queueSize);
        }
    }

    private void initInternalQueues(Integer consumingThreads)
    {
        IntStream.range(0, consumingThreads).forEach(i -> internalQueues.put(i, new QueueIOQ<>(internalQueuesMaxSize)));
    }



    public void startConsuming()
    {
        this.multiThreadExecutor = initMultiThreadExecutor(maxInternalThreads, multiThreadQueueSize);

        initInternalQueues(maxInternalThreads);
        initNewConsumerThread(maxInternalThreads);

        singleExecutor = initSingleThreadExecutor();
        singleExecutor.exec(buildMainTask());
    }



    private IQueueIOExecutorTask buildMainTask()
    {
        return () -> {
            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    E elm = inputQueue.take();
                    receivedObjectNotification(elm);
                    internalQueues.get(getNextQueueToBindIndex()).add(elm);
                }
                catch(Exception ex) {
                    log.error("buildMainTask error {}", ex.toString());
                }
            }
            return null;
        };
    }

    private IQueueIOExecutorTask buildInternalReceiverTask(BlockingQueue<E> queue, IQueueIOIngestionTask<E> ingestionTask)
    {
        return () -> {

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    E elm = queue.take();
                    ingestionTask.ingest(Collections.singletonList(elm));
                }
                catch(Exception ex) {
                    log.error("buildInternalReceiverTask error {}", ex.toString());
                }
            }
            return null;
        };
    }


    private IQueueIOExecutorTask buildInternalReceiverTaskSizeBatching(BlockingQueue<E> queue,
                                                                         IQueueIOIngestionTask<E> ingestionTask,
                                                                         int chunkSize,
                                                                         int flushTimeout,
                                                                         TimeUnit flushTimeUnit)
    {
        return () -> {

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    List<E> collection = new ArrayList<>();
                    Queues.drain(queue, collection, chunkSize, flushTimeout, flushTimeUnit);
                    ingestionTask.ingest(collection);
                }
                catch(Exception ex) {
                    log.error("buildInternalReceiverTaskSizeBatching error {}", ex.toString());
                }
            }
            return null;
        };
    }


    private IQueueIOExecutorTask buildInternalReceiverTaskByteBatching(BlockingQueue<E> queue,
                                                                         IQueueIOIngestionTask<E> ingestionTask,
                                                                         IQueueIOAccumulatorFactory<E> accumulatorFactory,
                                                                         int flushTimeout,
                                                                         TimeUnit flushTimeUnit)
    {
        return () -> {

            IQueueIOAccumulator<E> accumulator = accumulatorFactory.build();

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    long deadline = System.nanoTime() + flushTimeUnit.toNanos(flushTimeout);
                    while (accumulator.hasSpaceAvailable()) {

                        E elm = queue.poll(1, TimeUnit.NANOSECONDS);

                        if (elm == null) { // not enough elements immediately available; will have to poll
                            E e = queue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                            if (e == null) {
                                break; // we already waited enough, and there are no more elements in sight
                            }
                            accumulator.add(e);
                        }else{
                            accumulator.add(elm);
                        }

                    }

                    ingestionTask.ingest(accumulator.getRecords());
                    accumulator = accumulatorFactory.build();
                }
                catch(Exception ex) {
                    log.error("buildInternalReceiverTaskByteBatching error {}", ex.toString());
                }
            }
            return null;
        };
    }


    @Override
    public int getInternalThreads() {
        return maxInternalThreads;
    }

    @Override
    public <I1> void producedObjectNotification(I1 object) {
        queueIOMetric.getMetricProducedElements().incrementValue();
    }

    @Override
    public <I1> void producedObjectsNotification(List<I1> object) {
        queueIOMetric.getMetricProducedElements().incrementValue(object.size());

    }

    @Override
    public void receivedObjectNotification(E object) {
        queueIOMetric.getMetricReceivedElements().incrementValue();
    }

    @Override
    public void producedBytesNotification(byte[] object) {
        queueIOMetric.getMetricProducedBytes().setValue((long) object.length);
    }

    @Override
    public void receivedBytesNotification(byte[] object) {
        queueIOMetric.getMetricReceivedBytes().setValue((long) object.length);
    }

    @Override
    public <E1> void produce(E1 elm, Class<E1> clazz) {
        producedObjectNotification(elm);
        outputQueues.pushInQueue(clazz, elm);
    }

    @Override
    public <E1> void produceAll(List<E1> elms , Class<E1> clazz) {
        producedObjectsNotification(elms);
        outputQueues.pushAllInQueue(clazz, elms);
    }

    @Override
    public void updateMetrics() {
        queueIOMetric.setMetricExecutorQueueSize(multiThreadExecutor.getQueue());
        queueIOMetric.setMetricInputQueueSizeValue(singleExecutor.getQueue());
        queueIOMetric.setMetricInternalQueuesAVGSize(internalQueues
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()));
    }

    @Override
    public void registerMetrics(String appNamespace) {
        queueIOMetric.registerMetrics(appNamespace);
    }

    @Override
    public void flush() {
        log.info("flush called!");

    }

    @Override
    public void onDestroy() {
        log.info("onDestroy called!");
    }
}
