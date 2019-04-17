package com.fulmicotone.qio;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.utils.kinesis.streams.producer.KinesisStreamsQIOService;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.KinesisStreamsAccumulatorFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.accumulators.generic.BasicKinesisStreamsStringMapper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.ExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.HashProviderFactory;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.interfaces.IStreamShardHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.utils.HashKeyRangeHelper;
import com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.utils.RoundRobinStreamShardHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.Assert;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


@RunWith(JUnit4.class)
public class TestKinesisStreamsQIO extends TestUtils{


    public class KinesisStreamsQIOServiceTest extends KinesisStreamsQIOService<String> {

        BiConsumer<ByteBuffer, String> putRecordExplicitHashCallback;
        Consumer<ByteBuffer> putRecordCallback;

        public KinesisStreamsQIOServiceTest(Class<String> clazz, Integer threadSize) {
            super(clazz, threadSize);
        }

        public KinesisStreamsQIOServiceTest(Class<String> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<String, ByteBuffer> transformFunction) {
            super(clazz, threadSize, outputQueues, transformFunction);
        }

        public KinesisStreamsQIOServiceTest(Class<String> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<String, ByteBuffer> transformFunction) {
            super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
        }

        public KinesisStreamsQIOServiceTest withPutRecordExplicitHashCallback(BiConsumer<ByteBuffer, String> putRecordCallback){
            this.putRecordExplicitHashCallback = putRecordCallback;
            return this;
        }

        public KinesisStreamsQIOServiceTest withPutRecordCallback(Consumer<ByteBuffer> putRecordCallback){
            this.putRecordCallback = putRecordCallback;
            return this;
        }


        protected void putRecord(ByteBuffer record, String explicitHashKey) {

            if(putRecordExplicitHashCallback != null){
                putRecordExplicitHashCallback.accept(record, explicitHashKey);
            }
        }

        protected void putRecord(ByteBuffer record) {

            if(putRecordCallback != null){
                putRecordCallback.accept(record);
            }
        }
    }


    public class HashProviderFactoryTest extends HashProviderFactory {

        private List<HashKeyRange> fakeShardRanges;

        public HashProviderFactoryTest(AmazonKinesis amazonKinesis, String streamName, IStreamShardHelper iStreamShardHelper) {
            super(amazonKinesis, streamName, iStreamShardHelper);
        }

        public HashProviderFactoryTest(List<HashKeyRange> fakeShardRanges, IStreamShardHelper iStreamShardHelper) {
            super(null, null, iStreamShardHelper);
            this.fakeShardRanges = fakeShardRanges;
            init();
        }

        @Override
        protected void init() {

            if(fakeShardRanges == null){
                return;
            }

            Set<IExplicitShardKeyHelper> set = new HashSet<>();
            for(int i=0; i<fakeShardRanges.size(); i++){
                set.add(new ExplicitShardKeyHelper("shard-0000"+i, fakeShardRanges.get(i)));
            }

            shardKeyHelper.withStreamName(null)
                    .withHelpers(set);
        }
    }


    @Test
    public void test_Put_Record_No_Hash_Provider(){

        double recordMaxSize = 100_000;
        int flushSeconds = 10;

        String streamName = "FAKE_STREAM";
        KinesisStreamsAccumulatorFactory<String> factory = KinesisStreamsAccumulatorFactory.getBasicRecordFactory(recordMaxSize, new BasicKinesisStreamsStringMapper<>());
        TransferQueue<ByteBuffer> producedRecords = new LinkedTransferQueue<>();


        // BOOTSTRAP SERVICE
        KinesisStreamsQIOServiceTest kinesisStreamsQIOServiceTest = new KinesisStreamsQIOServiceTest(String.class, 2)
                .withStreamName(streamName)
                .withByteBatchingPerConsumerThread(factory, flushSeconds, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        kinesisStreamsQIOServiceTest.withPutRecordCallback(producedRecords::add);


        // START CONSUMING
        kinesisStreamsQIOServiceTest.startConsuming();


        // GENERATE FAKE DATAS: Expected 10MB in size
        this.tenByteStrings(1_000_000)
                .forEach(i -> kinesisStreamsQIOServiceTest.getInputQueue().add(i));


        try {
            Thread.sleep((long)((flushSeconds+1)*1000));


            List<ByteBuffer> elementsProduced = new ArrayList<>();
            producedRecords.drainTo(elementsProduced);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .filter(r -> r.array().length <= recordMaxSize)
                    .count() == elementsProduced.size(), "Single record have size > "+recordMaxSize);

            // EXPECT THAT PRODUCED RECORD HAS > 10MB IN SIZE (Because of json conversion)
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(r -> r.array().length)
                    .sum() > (1_000_000*10), "All records have size > "+recordMaxSize);

        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }


    @Test
    public void test_Put_Record_Hash_Provider(){

        double recordMaxSize = 100_000;
        int flushSeconds = 10;

        HashKeyRange shard1Range =  new HashKeyRange().withStartingHashKey("0").withEndingHashKey("140282366920938463463374607431768211455");
        HashKeyRange shard2Range =  new HashKeyRange().withStartingHashKey("140282366920938463463374607431768211456").withEndingHashKey("340282366920938463463374607431768211455");

        String streamName = "FAKE_STREAM";
        List<HashKeyRange> hashKeyRanges = Arrays.asList(
                shard1Range,
                shard2Range
        );
        KinesisStreamsAccumulatorFactory<String> factory = KinesisStreamsAccumulatorFactory.getBasicRecordFactory(recordMaxSize, new BasicKinesisStreamsStringMapper<>());
        TransferQueue<ByteBuffer> producedRecords = new LinkedTransferQueue<>();
        TransferQueue<String> explicitHashKeys = new LinkedTransferQueue<>();
        HashProviderFactoryTest hashProviderFactoryTest = new HashProviderFactoryTest(hashKeyRanges, new RoundRobinStreamShardHelper());


        // BOOTSTRAP SERVICE
        KinesisStreamsQIOServiceTest kinesisStreamsQIOServiceTest = new KinesisStreamsQIOServiceTest(String.class, 2)
                .withStreamName(streamName)
                .withExplicitHashProviderFactory(hashProviderFactoryTest)
                .withByteBatchingPerConsumerThread(factory, flushSeconds, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        kinesisStreamsQIOServiceTest.withPutRecordExplicitHashCallback((byteBuffer, s) -> {
            producedRecords.add(byteBuffer);
            explicitHashKeys.add(s);
        });


        // START CONSUMING
        kinesisStreamsQIOServiceTest.startConsuming();


        // GENERATE FAKE DATAS: 10 MB Strings
        this.tenByteStrings(1_000_000)
                .forEach(i -> kinesisStreamsQIOServiceTest.getInputQueue().add(i));


        try {
            Thread.sleep((long)((flushSeconds+1)*1000));


            List<ByteBuffer> elementsProduced = new ArrayList<>();
            producedRecords.drainTo(elementsProduced);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .filter(r -> r.array().length <= recordMaxSize)
                    .count() == elementsProduced.size(), "Single record have size > "+recordMaxSize);

            // EXPECT THAT PRODUCED RECORD HAS > 10MB IN SIZE (Because of json conversion)
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(r -> r.array().length)
                    .sum() > (1_000_000*10), "All records record have size > "+recordMaxSize);

            // EXPECT THAT HALF OF HASH KEYS BELONGS TO ONE SHARD AND THE OTHER HALF TO ANOTHER.
            List<String> keys = new ArrayList<>();
            explicitHashKeys.drainTo(keys);

            long shard1KeyOccurences = keys.stream()
                    .filter(k -> HashKeyRangeHelper.isRangeBetween(k, shard1Range))
                    .count();

            long shard2KeyOccurences = keys.stream()
                    .filter(k -> HashKeyRangeHelper.isRangeBetween(k, shard2Range))
                    .count();

            Assert.isTrue(shard1KeyOccurences+shard2KeyOccurences == keys.size(), "Hash keys are not distributed equally");


        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }


}
