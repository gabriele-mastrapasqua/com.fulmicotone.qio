package com.fulmicotone.qio;


import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.models.OutputQueues;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;

@RunWith(JUnit4.class)
public class BatchingTests extends TestUtils{



    @Test
    public void testSizeBatching_CASE_FLUSH() throws InterruptedException {

        int elements = 40;
        int threadSize = 4;

        int batchSize = 20;
        int flushSeconds = 5;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class,
                threadSize,
                100_000,
                new OutputQueues().withQueue(String.class, outputQueue),
                t -> t)
                .withSizeBatchingPerConsumerThread(batchSize, flushSeconds, timeUnit);
        stringQueueIO.startConsuming();


        stringsGenerator(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        Thread.sleep((long)((flushSeconds+1)*1000));


        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // EXPECT 4 ELEMENTS IN
        Assert.isTrue(results.size() == elements, "Elements are:"+results.size()+" expected "+elements);

        // Mapping
        Map<String, Long> countingMap = results
                .stream()
                .collect(Collectors.groupingBy(s -> s.split("=")[0], Collectors.counting()));
        Assert.isTrue(countingMap.keySet().size() == threadSize, "Key size are less than threadSize");


    }


    @Test
    public void testSizeBatching_CASE_BATCH_BEFORE_FLUSH() {

        int elements = 100;
        int threadSize = 4;

        int batchSize = 50;
        int flushSeconds = 5;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, threadSize, 100_000,new OutputQueues().withQueue(String.class, outputQueue), t -> t)
                .withSizeBatchingPerConsumerThread(batchSize, flushSeconds, timeUnit);
        stringQueueIO.startConsuming();


        long realDeadline = System.currentTimeMillis()+((flushSeconds)*1000)-100;
        long resultDeadline = System.currentTimeMillis()+((flushSeconds+1)*1000);

        // 10 bytes STRINGS * ELEMENTS
        stringsGenerator(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        while (realDeadline > System.currentTimeMillis()){
            List<String> collection = new ArrayList<>();
            outputQueue.drainTo(collection);
            if(collection.size() > 0){
                Assert.isTrue(false, "Flush does not work");
            }
        }

        while (resultDeadline > System.currentTimeMillis()){
        }


        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);


        // EXPECT 4 ELEMENTS IN
        Assert.isTrue(results.size() == elements, "Elements are:"+results.size()+" expected "+elements);

        // Mapping
        Map<String, Long> countingMap = results
                .stream()
                .collect(Collectors.groupingBy(s -> s.split("=")[0], Collectors.counting()));
        Assert.isTrue(countingMap.keySet().size() == threadSize, "Key size are less than threadSize");
        Assert.isTrue(countingMap.entrySet()
                .stream()
                .filter(e -> e.getValue() == (elements/threadSize))
                .count() == threadSize, "Threads counts different objects");

    }



    @Test
    public void testByteBatching_CASE_FLUSH() throws InterruptedException {

        int elements = 40;
        int threadSize = 4;

        int flushSeconds = 5;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        System.out.println("JAVA THREAD");

        // 100 bytes accumulator
        IQueueIOAccumulatorFactory<String, String> tenKAccumulatorFactory = () -> new StringSizeAccumulator(100);


        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, threadSize, 100_000,
                new OutputQueues().withQueue(String.class, outputQueue),
                t -> t)
                .withByteBatchingPerConsumerThread(tenKAccumulatorFactory, flushSeconds, timeUnit);
        stringQueueIO.startConsuming();


        long deadline = System.currentTimeMillis()+((flushSeconds+1)*1000);

        // 10 bytes STRINGS * ELEMENTS
        tenByteStrings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        while (deadline > System.currentTimeMillis()){
            // WAIT
        }


        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // EXPECT 4 ELEMENTS IN
        Assert.isTrue(results.size() == elements, "Elements are:"+results.size()+" expected "+elements);

    }


    @Test
    public void testByteBatching_CASE_BATCH_BEFORE_FLUSH() {

        int elements = 9;
        int threadSize = 4;

        int flushSeconds = 5;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        System.out.println("JAVA THREAD");

        // 100 bytes accumulator
        IQueueIOAccumulatorFactory<String, String> tenKAccumulatorFactory = () -> new StringSizeAccumulator(100);


        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, threadSize, 100_000,new OutputQueues().withQueue(String.class, outputQueue), t -> t)
                .withByteBatchingPerConsumerThread(tenKAccumulatorFactory, flushSeconds, timeUnit);
        stringQueueIO.startConsuming();


        long realDeadline = System.currentTimeMillis()+((flushSeconds)*1000)-100;
        long resultDeadline = System.currentTimeMillis()+((flushSeconds+1)*1000);

        // 10 bytes STRINGS * ELEMENTS
        tenByteStrings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        while (realDeadline > System.currentTimeMillis()){
            List<String> collection = new ArrayList<>();
            outputQueue.drainTo(collection);
            if(collection.size() > 0){
                Assert.isTrue(false, "Flush does not work");
            }
        }

        while (resultDeadline > System.currentTimeMillis()){
        }


        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // EXPECT 4 ELEMENTS IN
        Assert.isTrue(results.size() == elements, "Elements are:"+results.size()+" expected "+elements);
    }
}
