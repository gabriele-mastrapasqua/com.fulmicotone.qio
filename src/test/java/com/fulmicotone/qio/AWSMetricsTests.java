//package com.fulmicotone.qio;
//
//
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
//import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
//import com.fulmicotone.qio.components.metrics.utils.CounterToRateMetricTransformFixed;
//import com.fulmicotone.qio.models.OutputQueues;
//import com.netflix.servo.publish.*;
//import com.netflix.servo.publish.cloudwatch.CloudWatchMetricObserver;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//import org.springframework.util.Assert;
//
//import java.util.HashSet;
//import java.util.Set;
//import java.util.concurrent.LinkedTransferQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TransferQueue;
//
//
///**
// * Use this test with one AWS Account.
// * After launching it you should see into Cloudwatch console (https://console.aws.amazon.com/cloudwatch/home?region=<REGION>#metricsV2:)
// * one new namespace called as the value of variable 'cloudwatchNamespace'.
// *
// */
//
//@RunWith(JUnit4.class)
//public class AWSMetricsTests extends TestUtils {
//
//    private static final String ACCESS_KEY_ENV = "ACCESS_KEY";
//    private static final String SECRET_KEY_ENV = "SECRET_KEY";
//    private static final TimeUnit cloudwatchUpdateTimeUnit = TimeUnit.SECONDS;
//    private static final int cloudwatchUpdateValue = 5;
//
//    @Before
//    public void startCustomMetricUpdates()
//    {
//        String cloudwatchNamespace = "QIOServices-Test";
//        String accessKey = System.getenv(ACCESS_KEY_ENV);
//        String secretKey = System.getenv(SECRET_KEY_ENV);
//
//        if(accessKey == null || secretKey == null){
//            throw new RuntimeException("AWS Client & Secret key must exists");
//        }
//
//        AmazonCloudWatch client = AmazonCloudWatchClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
//                .build();
//
//        PollScheduler scheduler = PollScheduler.getInstance();
//        scheduler.start();
//
//        CloudWatchMetricObserver observer = new CloudWatchMetricObserver("Component", cloudwatchNamespace, client);
//        MetricObserver transform = new CounterToRateMetricTransformFixed(observer, cloudwatchUpdateValue, cloudwatchUpdateTimeUnit);
//
//
//        PollRunnable task = new PollRunnable(
//                new MonitorRegistryMetricPoller(),
//                BasicMetricFilter.MATCH_ALL,
//                transform);
//
//        scheduler.addPoller(task, cloudwatchUpdateValue, cloudwatchUpdateTimeUnit);
//
//
//    }
//
//
//
//    @Test
//    public void test() throws InterruptedException {
//
//        String appNamespace = "qio-app-test";
//
//        int elements = 10_000_000;
//        int threadSize = 3;
//
//
//        TransferQueue<String> outputQueue = new LinkedTransferQueue<>();
//
//
//        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(
//                String.class,
//                threadSize, 100_000,
//                new OutputQueues().withQueue(String.class, outputQueue)
//        );
//
//        // REGISTER METRIC FIRST
//        stringQueueIO.registerMetrics(appNamespace);
//        stringQueueIO.startConsuming();
//
//
//        long ms = System.currentTimeMillis();
//        stringsGenerator(elements).forEach(s -> {
//            try {
//                stringQueueIO.getInputQueue().put(s);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//
//
//        while(outputQueue.size() < elements){
//
//            if ( outputQueue.size() % 1000 == 0 )
//                stringQueueIO.updateMetrics();
//        }
//
//        System.out.println("Executed in "+(System.currentTimeMillis()-ms)+" ms");
//
//        Set<String> results = new HashSet<>();
//        outputQueue.drainTo(results);
//
//
//        Thread.sleep(10_000);
//
//        Assert.isTrue(results.size() == elements, "Elements and results are differents");
//
//
//    }
//
//}
