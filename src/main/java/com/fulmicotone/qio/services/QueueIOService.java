package com.fulmicotone.qio.services;


import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulator;
import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.components.metrics.QueueIOMetric;
import com.fulmicotone.qio.factories.QueueIOExecutorFactory;
import com.fulmicotone.qio.interfaces.*;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public abstract class QueueIOService<E, T> implements IQueueIOService<E, T> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private Class<E> clazz;


    private QueueIOQ<E> inputQueue;
    private OutputQueues outputQueues;
    protected IQueueIOExecutor multiThreadExecutor;
    protected IQueueIOExecutor singleExecutor;
    private Map<Integer, QueueIOQ<E>> internalQueues = new HashMap<>();
    private Map<Integer, AtomicLong> internalQueuesReceivedElements = new HashMap<>();

    private IQueueIOExecutorFactory singleThreadExecutorFactory = new QueueIOExecutorFactory();
    private IQueueIOExecutorFactory multiThreadExecutorFactory = new QueueIOExecutorFactory();

    private QueueIOMetric queueIOMetric;
    private int internalQueueThreadCreationIndex = 0;
    private int maxInternalThreads;
    private int internalQueuesMaxSize = 20_000_000;
    private int multiThreadQueueSize;
    private int chunkSize = 100;
    private int flushTimeout = 30;
    private TimeUnit flushTimeUnit = TimeUnit.SECONDS;
    private boolean sizeBatchingEnabled = false;
    private boolean byteBatchingEnabled = false;
    private IQueueIOAccumulatorFactory<E, T> accumulatorFactory;
    private IQueueIOTransform<E, T> transformFunction;
    private Future<?> consumingTask;





    public QueueIOService(Class<E> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<E, T> transformFunction)
    {
        this(clazz, threadSize, 100_000_000, outputQueues, transformFunction);
    }

    public QueueIOService(Class<E> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<E, T> transformFunction)
    {
        this.clazz = clazz;
        this.inputQueue = new QueueIOQ<>();
        this.transformFunction = transformFunction;
        this.outputQueues = outputQueues;
        this.maxInternalThreads = threadSize;
        this.multiThreadQueueSize = multiThreadQueueSize;
        this.queueIOMetric = new QueueIOMetric(this);
    }



    public <I extends QueueIOService<E, T>> I withQueueIOMetric(QueueIOMetric metric){
        this.queueIOMetric = metric;
        return (I)this;
    }

    public <I extends QueueIOService<E, T>> I withSingleThreadExecutorFactory(IQueueIOExecutorFactory executorFactory){
        this.singleThreadExecutorFactory = executorFactory;
        return (I)this;
    }

    public <I extends QueueIOService<E, T>> I withMultiThreadExecutorFactory(IQueueIOExecutorFactory executorFactory){
        this.multiThreadExecutorFactory = executorFactory;
        return (I)this;
    }

    public <I extends QueueIOService<E, T>> I withInternalQueuesMaxSize(int internalQueuesMaxSize){
        this.internalQueuesMaxSize = internalQueuesMaxSize;
        return (I)this;
    }

    public <I extends QueueIOService<E, T>> I withSizeBatchingPerConsumerThread(int chunkSize, int flushTimeout, TimeUnit flushTimeUnit){
        sizeBatchingEnabled = true;
        this.chunkSize = chunkSize;
        this.flushTimeout = flushTimeout;
        this.flushTimeUnit = flushTimeUnit;
        return (I)this;
    }

    public <I extends QueueIOService<E, T>> I withByteBatchingPerConsumerThread(IQueueIOAccumulatorFactory<E, T> accumulatorFactory, int flushTimeout, TimeUnit flushTimeUnit){
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

    protected OutputQueues getOutputQueues() {
        return this.outputQueues;
    }

    @Override
    public String getUniqueKey() {
        return getClass().getSimpleName()+"<"+getInputClass().getSimpleName()+">";
    }

    public int getInternalQueueSize(int n)
    {
        QueueIOQ<E> queue = internalQueues.get(n);

        if(internalQueues.get(n) == null){
            return -1;
        }

        return queue.size();
    }

    public int getInternalQueuesMaxSize() {
        return internalQueuesMaxSize;
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
        return singleThreadExecutorFactory.createExecutor(getClass().getSimpleName()+"-st",1, 1_000_000);
    }

    private IQueueIOExecutor initMultiThreadExecutor(Integer consumingThreads, Integer queueSize){
        return multiThreadExecutorFactory.createExecutor(getClass().getSimpleName()+"-mt", consumingThreads, queueSize);
    }

    private void initInternalQueues(Integer consumingThreads)
    {
        IntStream.range(0, consumingThreads).forEach(i -> internalQueues.put(i, new QueueIOQ<>(internalQueuesMaxSize)));
        IntStream.range(0, consumingThreads).forEach(i -> internalQueuesReceivedElements.put(i, new AtomicLong()));
    }

    private void initExecutorsIfNeeded(){
        if(multiThreadExecutor == null){
            this.multiThreadExecutor = initMultiThreadExecutor(maxInternalThreads, multiThreadQueueSize);
            initInternalQueues(maxInternalThreads);
            initNewConsumerThread(maxInternalThreads);
        }

        if(this.singleExecutor == null){
            this.singleExecutor = initSingleThreadExecutor();
        }
    }


    public void startConsuming()
    {
        initExecutorsIfNeeded();
        consumingTask = singleExecutor.exec(buildConsumingTask());
    }

    public void stopConsuming(){
        consumingTask.cancel(true);
        consumingTask = null;
    }


    public boolean isRunning(){
        return consumingTask != null;
    }


    private void sendObjectToInternalQueue(E elm){
        int idx = getNextQueueToBindIndex();
        internalQueues.get(idx).add(elm);
        internalQueuesReceivedElements.get(idx).incrementAndGet();
    }


    private IQueueIOExecutorTask buildConsumingTask()
    {
        return () -> {
            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    E elm = inputQueue.take();
                    receivedObjectNotification(elm);
                    sendObjectToInternalQueue(elm);
                }
                catch(Exception ex) {
                    log.error("buildConsumingTask error {}", ex.toString());
                }
            }
            return null;
        };
    }

    private IQueueIOExecutorTask buildInternalReceiverTask(BlockingQueue<E> queue, IQueueIOIngestionTask<T> ingestionTask)
    {
        return () -> {

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    E elm = queue.take();
                    ingestionTask.ingest(Collections.singletonList(transformFunction.apply(elm)));
                }
                catch(Exception ex) {
                    log.error("buildInternalReceiverTask error {}", ex.toString());
                    ex.printStackTrace();
                }
            }
            return null;
        };
    }


    private IQueueIOExecutorTask buildInternalReceiverTaskSizeBatching(BlockingQueue<E> queue,
                                                                         IQueueIOIngestionTask<T> ingestionTask,
                                                                         int chunkSize,
                                                                         int flushTimeout,
                                                                         TimeUnit flushTimeUnit)
    {
        return () -> {

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    E elm;
                    List<E> collection = new ArrayList<>();
                    long deadline = System.nanoTime() + flushTimeUnit.toNanos(flushTimeout);

                    do{
                        elm = queue.poll(1, TimeUnit.NANOSECONDS);

                        if (elm == null) { // not enough elements immediately available; will have to poll
                            elm = queue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                            if (elm == null) {
                                break; // we already waited enough, and there are no more elements in sight
                            }
                            collection.add(elm);
                        }else{
                            collection.add(elm);
                        }
                    }
                    while (collection.size() < chunkSize);

                    if(collection.size() > 0){
                        ingestionTask.ingest(collection.stream().map(transformFunction).collect(Collectors.toList()));
                    }

                }
                catch(Exception ex) {
                    log.error("buildInternalReceiverTaskSizeBatching error {}", ex.toString());
                    ex.printStackTrace();
                }
            }
            return null;
        };
    }


    private IQueueIOExecutorTask buildInternalReceiverTaskByteBatching(BlockingQueue<E> queue,
                                                                         IQueueIOIngestionTask<T> ingestionTask,
                                                                         IQueueIOAccumulatorFactory<E, T> accumulatorFactory,
                                                                         int flushTimeout,
                                                                         TimeUnit flushTimeUnit)
    {
        return () -> {

            IQueueIOAccumulator<E, T> accumulator = accumulatorFactory.build();

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    long deadline = System.nanoTime() + flushTimeUnit.toNanos(flushTimeout);
                    boolean isAccumulatorAvailable;
                    E elm;

                    do{
                        elm = queue.poll(1, TimeUnit.NANOSECONDS);

                        if (elm == null) { // not enough elements immediately available; will have to poll
                            elm = queue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                            if (elm == null) {
                                break; // we already waited enough, and there are no more elements in sight
                            }
                            isAccumulatorAvailable = accumulator.add(elm);
                        }else{
                            isAccumulatorAvailable = accumulator.add(elm);
                        }
                    }
                    while (isAccumulatorAvailable);


                    List<T> records = accumulator.getRecords();

                    if(records.size() > 0){
                        ingestionTask.ingest(records);
                    }


                    accumulator = accumulatorFactory.build();
                    if(elm != null){ accumulator.add(elm); }
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
        queueIOMetric.setMetricMultiExecutorQueueSize(multiThreadExecutor.getQueueSize());
        queueIOMetric.setMetricInputQueueSizeValue(singleExecutor.getQueueSize());
        queueIOMetric.setMetricInternalQueuesAVGSize(internalQueues
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()));
    }

    @Override
    public QueueIOMetric getMetrics() {
        return queueIOMetric;
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
