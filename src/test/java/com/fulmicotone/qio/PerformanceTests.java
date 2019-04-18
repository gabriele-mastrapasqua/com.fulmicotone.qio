package com.fulmicotone.qio;


import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.models.OutputQueues;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;

@RunWith(JUnit4.class)
public class PerformanceTests extends TestUtils{

    private int elements = 1_000_000;
    private int byteBatchingSize = 1_000;
    private int threadSize = 4;

    @Test
    public void test() {


        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, threadSize, 100_000, new OutputQueues().withQueue(String.class, outputQueue), t -> t);
        stringQueueIO.startConsuming();


        tenByteStrings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long ms = System.currentTimeMillis();

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
    public void test_BATCH() {



        System.out.println("JAVA THREAD - BYTE BATCH");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();
        // 100 bytes accumulator
        IQueueIOAccumulatorFactory<String, String> tenKAccumulatorFactory = () -> new StringSizeAccumulator(byteBatchingSize);


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, threadSize, 100_000, new OutputQueues().withQueue(String.class, outputQueue), t -> t)
                .withByteBatchingPerConsumerThread(tenKAccumulatorFactory, 1, TimeUnit.SECONDS);
        stringQueueIO.startConsuming();


        tenByteStrings(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long ms = System.currentTimeMillis();

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




}
