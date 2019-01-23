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
public class Tests {


    private static final long MEGABYTE = 1024L * 1024L;


    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    private List<String> strings(int max)
    {
        List<String> arrayList = new ArrayList<>();
        for(int i = 0; i<max; i++){
            arrayList.add("String"+i);
        }
        return arrayList;
    }

    private List<String> tenByteStrings(int max)
    {
        List<String> arrayList = new ArrayList<>();
        for(int i = 0; i<max; i++){
            arrayList.add("0123456789");
        }
        return arrayList;
    }



    @Test
    public void test() {

        int elements = 100_000;
        int threadSize = 4;

        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue));
        stringQueueIO.startConsuming(threadSize, 100_000);


        long ms = System.currentTimeMillis();
        strings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        while(outputQueue.size() < elements){

        }

        System.out.println("Executed in "+(System.currentTimeMillis()-ms)+" ms");

        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // ALL ELEMENTS ARE DIFFERENT
        Assert.isTrue(results.size() == elements);

        // Mapping
        Map<String, Long> countingMap = results
                .stream()
                .collect(Collectors.groupingBy(s -> s.split("=")[0], Collectors.counting()));
        Assert.isTrue(countingMap.keySet().size() == threadSize, "Key size are less than threadSize");
        Assert.isTrue(countingMap.entrySet()
                .stream()
                .filter(e -> e.getValue() == (elements/threadSize))
                .count() == threadSize, "Threads counts different objects");


        Runtime runtime = Runtime.getRuntime();
        // Run the garbage collector
        runtime.gc();
        // Calculate the used memory
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory is bytes: " + memory);
        System.out.println("Used memory is megabytes: "
                + bytesToMegabytes(memory));
    }


    @Test
    public void testFiber() {

        int elements = 100_000;
        int threadSize = 4;

        System.out.println("QUASAR FIBERS");


        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue))
                .withQuasar(true);
        stringQueueIO.startConsuming(threadSize, 100_000);


        long ms = System.currentTimeMillis();
        strings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        while(outputQueue.size() < elements){

        }

        System.out.println("Executed in "+(System.currentTimeMillis()-ms)+" ms");

        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // ALL ELEMENTS ARE DIFFERENT
        Assert.isTrue(results.size() == elements);

        // Mapping
        Map<String, Long> countingMap = results
                .stream()
                .collect(Collectors.groupingBy(s -> s.split("=")[0], Collectors.counting()));
        Assert.isTrue(countingMap.keySet().size() == threadSize, "Key size are less than threadSize");
        Assert.isTrue(countingMap.entrySet()
                .stream()
                .filter(e -> e.getValue() == (elements/threadSize))
                .count() == threadSize, "Threads counts different objects");

        Runtime runtime = Runtime.getRuntime();
        // Run the garbage collector
        runtime.gc();
        // Calculate the used memory
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory is bytes: " + memory);
        System.out.println("Used memory is megabytes: "
                + bytesToMegabytes(memory));
    }



    @Test
    public void testSizeBatching_CASE_FLUSH() throws InterruptedException {

        int elements = 40;
        int threadSize = 4;

        int batchSize = 20;
        int flushSeconds = 5;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue))
                .withSizeBatchingPerConsumerThread(batchSize, flushSeconds, timeUnit);
        stringQueueIO.startConsuming(threadSize, 100_000);


        strings(elements).forEach(s -> {
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


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue))
                .withSizeBatchingPerConsumerThread(batchSize, flushSeconds, timeUnit);
        stringQueueIO.startConsuming(threadSize, 100_000);


        long ms = System.currentTimeMillis();
        strings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        while(outputQueue.size() < elements){

        }

        Assert.isTrue((System.currentTimeMillis()-ms) > flushSeconds*1000, "Time passed is > than "+flushSeconds+" "+timeUnit.name());

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
        IQueueIOAccumulatorFactory<String> tenKAccumulatorFactory = () -> new StringSizeAccumulator(100);


        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue))
                .withByteBatchingPerConsumerThread(tenKAccumulatorFactory, flushSeconds, timeUnit);
        stringQueueIO.startConsuming(threadSize, 100_000);


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
        IQueueIOAccumulatorFactory<String> tenKAccumulatorFactory = () -> new StringSizeAccumulator(100);


        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, new OutputQueues().withQueue(String.class, outputQueue))
                .withByteBatchingPerConsumerThread(tenKAccumulatorFactory, flushSeconds, timeUnit);
        stringQueueIO.startConsuming(threadSize, 100_000);


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
