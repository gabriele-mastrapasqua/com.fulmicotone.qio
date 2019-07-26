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
public class StartStopTests extends TestUtils{



    @Test
    public void testStartStop() throws InterruptedException {

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

        Assert.isTrue(stringQueueIO.isRunning(), "Is not running!");


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


        // STOP
        stringQueueIO.stopConsuming();

        Assert.isTrue(!stringQueueIO.isRunning(), "Is running!");


        // START AGAIN
        stringQueueIO.startConsuming();



        stringsGenerator(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread.sleep((long)((flushSeconds+1)*1000));


        results = new HashSet<>();
        outputQueue.drainTo(results);

        // EXPECT 4 ELEMENTS IN
        Assert.isTrue(results.size() == elements, "Elements are:"+results.size()+" expected "+elements);

        // Mapping
        countingMap = results
                .stream()
                .collect(Collectors.groupingBy(s -> s.split("=")[0], Collectors.counting()));
        Assert.isTrue(countingMap.keySet().size() == threadSize, "Key size are less than threadSize");



    }

}
