package com.fulmicotone.qio.v2;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fulmicotone.qio.v2.functions.FnKinesisJsonStringTransform;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOService;
import com.fulmicotone.qio.utils.kinesis.streams.common.KinesisJsonListDecoder;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.KinesisConsumerQIOService;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.RecordProcessorFactory;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.models.KCLConsumer;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.models.KCLConsumerEntry;
import com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.models.KCLPartitionKey;
import com.fulmicotone.qio.utils.kinesis.streams.producer.KinesisStreamsQIOService;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.KinesisStreamsAccumulatorFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic.BasicKinesisStreamsByteMapper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic.BasicKinesisStreamsJsonStringMapper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.HashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.utils.RoundRobinStreamShardHelper;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.util.*;
import java.util.concurrent.*;

@RunWith(JUnit4.class)
public class TestKinesisProducerConsumerV2 {
    private AwsBasicCredentials credentials = AwsBasicCredentials.create(
            System.getenv("AWS_ACCESS_KEY_ID"),
            System.getenv("AWS_SECRET_KEY"));
    private String applicationName = System.getenv("KINESIS_APPLICATION_NAME");
    private String inputStreamName = System.getenv("KINESIS_STREAM_NAME");
    private String partitionKey = System.getenv("KINESIS_PARTITION_KEY");
    private String region = "us-east-1";
    private String kinesisMetricLevel = "none";


    // TEST QIO Consumer Destination Queue
    private class DestinationQIO extends QueueIOService<String, String> {
        public DestinationQIO(Class<String> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<String, String> transformFunction) {
            super(clazz, threadSize, outputQueues, transformFunction);
        }

        @Override
        public IQueueIOIngestionTask<String> ingestionTask() {
            return new IQueueIOIngestionTask<String>() {
                @Override
                public Void ingest(List<String> list) {
                    System.out.println("Received "+list.size()+" strings");
                    return null;
                }
            };
        }
    }





    private KinesisProducer kinesisProducer(){
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(60000)
                .setMaxConnections(24)
                .setRequestTimeout(10000)
                .setRecordTtl(180000)
                .setCredentialsProvider(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                            System.getenv("AWS_ACCESS_KEY_ID"),
                            System.getenv("AWS_SECRET_KEY"))
        ))
                .setMetricsLevel(kinesisMetricLevel)
                .setRegion(region);

        return new KinesisProducer(config);
    }

    public KinesisAsyncClient newKinesisClient() {
        return KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                /*.httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(100))*/
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
        );

    }
            
    // test kinesis producer and consumer

    @Test
    public void testKPL_KCL_ProducerConsumer() throws Exception {

        // Kinesis Producer setup

        // with QIO: produce on kinesis gzipped records, then read from kinesis -> map to syncs -> http req with fibers
        double recordMaxSize = 100_000;
        int flushSeconds = 10;
        KinesisStreamsAccumulatorFactory<String> factory =  new KinesisStreamsAccumulatorFactory<>(recordMaxSize,
                new BasicKinesisStreamsJsonStringMapper<>(),
                new com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic.BasicKinesisStreamsRecordMapper(),
                new BasicKinesisStreamsByteMapper(),
                new com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic.BasicKinesisStreamsAccumulatorLengthFunction<>());

        KinesisAsyncClient amazonKinesis = newKinesisClient();
        KinesisProducer kinesisProducer = kinesisProducer();
        HashProviderFactory hashProviderFactory = new HashProviderFactory(amazonKinesis, inputStreamName,
                new RoundRobinStreamShardHelper());
        // QIO Kinesis Producer
        KinesisStreamsQIOService<String> kinesisStreamsQIOServiceTest = new KinesisStreamsQIOService<>(String.class, 2)
                .withKinesisProducer(kinesisProducer)
                .withStreamName(inputStreamName)
                .withPartitionKey(partitionKey)
                .withExplicitHashProviderFactory(hashProviderFactory)
                .withByteBatchingPerConsumerThread(factory, flushSeconds, TimeUnit.SECONDS);

        // START producer QIO
        kinesisStreamsQIOServiceTest.startConsuming();

        // insert data into producer:
        kinesisStreamsQIOServiceTest.getInputQueue().addAll(Arrays.asList("a", "b"));



        // 2 - Kinesis Consumer


        // next start kinesis client consumer


        DestinationQIO destinationQIO = new DestinationQIO(String.class, 2, null, t -> t);
        destinationQIO.startConsuming();


        KCLPartitionKey dmpSyncOperationKey = new KCLPartitionKey("sync");
        KCLConsumer<String, String> syncToHttpRequest = new KCLConsumer<String, String>(
                new KinesisJsonListDecoder(),
                new FnKinesisJsonStringTransform(),
                destinationQIO.getInputQueue(),
                String.class,
                "strConsumerOutput"
        );
        KCLConsumerEntry<String> syncEntry = new KCLConsumerEntry<>(dmpSyncOperationKey, Arrays.asList(syncToHttpRequest));


        // start consuming kinesis:
        Set<KCLConsumerEntry> consumerEntries = Sets.newHashSet(syncEntry);
        RecordProcessorFactory recordProcessorFactory = new com.fulmicotone.qio.utils.kinesis.streams.consumer.v1.RecordProcessorFactory()
                .withPossibleOutputs(consumerEntries)
                .withBackoffTime(100)
                .withCheckpointInterval(100)
                .withRetriesNumber(2)
                .withExecutor( new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>()) )
                ;

        KinesisConsumerQIOService kinesisConsumerQIOService = new KinesisConsumerQIOService(recordProcessorFactory, inputStreamName,
                applicationName, Region.US_EAST_1, 100);
        kinesisConsumerQIOService.startConsuming();


        Thread.sleep(100_000);
    }


}