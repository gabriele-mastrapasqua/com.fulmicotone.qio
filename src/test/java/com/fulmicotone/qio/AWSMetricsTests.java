package com.fulmicotone.qio;


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.fulmicotone.qio.components.metrics.utils.CounterToRateMetricTransformFixed;
import com.fulmicotone.qio.models.OutputQueues;
import com.netflix.servo.publish.*;
import com.netflix.servo.publish.cloudwatch.CloudWatchMetricObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Stream;

@RunWith(JUnit4.class)
public class AWSMetricsTests extends TestUtils {

    private static final String ACCESS_KEY_ENV = "ACCESS_KEY";
    private static final String SECRET_KEY_ENV = "SECRET_KEY";
    private static final TimeUnit cloudwatchUpdateTimeUnit = TimeUnit.SECONDS;
    private static final int cloudwatchUpdateValue = 5;

    @Before
    public void startCustomMetricUpdates()
    {
        String accessKey = System.getenv(ACCESS_KEY_ENV);
        String secretKey = System.getenv(SECRET_KEY_ENV);

        if(accessKey == null || secretKey == null){
            throw new RuntimeException("AWS Client & Secret key must exists");
        }

        AmazonCloudWatch client = AmazonCloudWatchClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();

        PollScheduler scheduler = PollScheduler.getInstance();
        scheduler.start();

        CloudWatchMetricObserver observer = new CloudWatchMetricObserver("Component", "QIOServices-Test", client);
        MetricObserver transform = new CounterToRateMetricTransformFixed(observer, cloudwatchUpdateValue, cloudwatchUpdateTimeUnit);


        PollRunnable task = new PollRunnable(
                new MonitorRegistryMetricPoller(),
                BasicMetricFilter.MATCH_ALL,
                transform);

        scheduler.addPoller(task, cloudwatchUpdateValue, cloudwatchUpdateTimeUnit);


    }



    @Test
    public void test()
    {
        int elements = 100_000;
        int threadSize = 4;

        System.out.println("JAVA THREAD");



        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();


        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(
                String.class,
                new OutputQueues().withQueue(String.class, outputQueue)
        );

        // REGISTER METRIC FIRST
        stringQueueIO.registerMetrics("fake-test");

        stringQueueIO.startConsuming(threadSize, 100_000);


        long ms = System.currentTimeMillis();
        stringsGenerator(elements).forEach(s -> {
            try {
                stringQueueIO.getInputQueue().put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        while(outputQueue.size() < elements){
            stringQueueIO.updateMetrics();
        }

        System.out.println("Executed in "+(System.currentTimeMillis()-ms)+" ms");

        Set<String> results = new HashSet<>();
        outputQueue.drainTo(results);

        // ALL ELEMENTS ARE DIFFERENT
        org.springframework.util.Assert.isTrue(results.size() == elements);




    }

}
