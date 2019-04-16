package com.fulmicotone.qio;

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.utils.kinesis.firehose.FirehoseQIOService;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.FirehoseAccumulatorFactory;
import com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.BasicFirehoseJsonStringMapper;
import com.fulmicotone.qio.utils.kinesis.firehose.enums.PutRecordMode;
import com.google.common.collect.Queues;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.fulmicotone.qio.utils.kinesis.firehose.accumulators.generic.BasicFirehoseRecordMapper.RECORD_MAX_SIZE_IN_BYTES;
import static com.fulmicotone.qio.utils.kinesis.firehose.accumulators.smartGZIP.SmartGZIPFirehoseRecordMapper.RECORD_COMPRESSED_MAX_SIZE_IN_BYTES;
import static com.fulmicotone.qio.utils.kinesis.firehose.consts.PutRecordLimits.PUT_BATCH_LIMIT_MB;
import static com.fulmicotone.qio.utils.kinesis.firehose.consts.PutRecordLimits.PUT_LIMIT_MB;

@RunWith(JUnit4.class)
public class TestFirehoseQIO extends TestUtils{


    public class FirehoseQIOServiceTest extends FirehoseQIOService<String> {

        BiConsumer<Record, String> putRecordCallback;
        BiConsumer<List<Record>, String> putRecordBatchCallback;

        public FirehoseQIOServiceTest(Class<String> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<String, Record> transformFunction) {
            super(clazz, threadSize, outputQueues, transformFunction);
        }

        public FirehoseQIOServiceTest(Class<String> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<String, Record> transformFunction) {
            super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
        }

        public FirehoseQIOServiceTest withPutRecordCallback(BiConsumer<Record, String> putRecordCallback){
            this.putRecordCallback = putRecordCallback;
            return this;
        }

        public FirehoseQIOServiceTest withPutRecordBatchCallback(BiConsumer<List<Record>, String> putRecordBatchCallback){
            this.putRecordBatchCallback = putRecordBatchCallback;
            return this;
        }


        protected void putRecord(Record record, String streamName) {
            Optional.ofNullable(putRecordCallback).ifPresent(c -> c.accept(record, streamName));
        }

        protected void putRecordBatch(List<Record> list, String streamName) {
            Optional.ofNullable(putRecordBatchCallback).ifPresent(c -> c.accept(list, streamName));
        }
    }


    @Test
    public void test_Put_Record_BATCH_SmartGZIP_Accumulator(){

        double byteSizeLimit = PUT_BATCH_LIMIT_MB;
        double recordMaxSize = RECORD_COMPRESSED_MAX_SIZE_IN_BYTES;
        PutRecordMode putRecordMode = PutRecordMode.BATCH;
        List<String> streamNames = Collections.singletonList("FAKE_STREAM");
        FirehoseAccumulatorFactory<String> factory = FirehoseAccumulatorFactory.getSmartGZIPFactory(byteSizeLimit, new BasicFirehoseJsonStringMapper<>());
        TransferQueue<List<Record>> producedRecords = new LinkedTransferQueue<>();


        // BOOSTRAP SERVICE
        FirehoseQIOServiceTest firehoseQIOService = new FirehoseQIOServiceTest(String.class, 2, null, null)
                .withStreamNames(streamNames)
                .withPutRecordMode(putRecordMode)
                .withByteBatchingPerConsumerThread(factory, 10, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        firehoseQIOService.withPutRecordBatchCallback((records, s) -> {
            producedRecords.add(records);
        });


        // START CONSUMING
        firehoseQIOService.startConsuming();


        // GENERATE FAKE DATAS
        this.tenByteStrings(10_000_000)
                .forEach(i -> {
                    firehoseQIOService.getInputQueue().add(i);
                });


        try {
            List<List<Record>> elementsProduced = new ArrayList<>();
            Queues.drain(producedRecords, elementsProduced, 10, 15, TimeUnit.SECONDS);


            // EXPECT THAT EVERY CHUNK HAS A TOTAL SUM OF BYTES < byteSizeLimit
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(s -> s.stream().mapToInt(record -> record.getData().array().length).sum())
                    .filter(sum -> sum < byteSizeLimit)
                    .count() == elementsProduced.size(), "Size > "+byteSizeLimit);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(r -> r.getData().array().length <= recordMaxSize)
                    .count() == elementsProduced.stream().mapToLong(Collection::size).sum(), "Single record have size > "+recordMaxSize);

        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }



    @Test
    public void test_Put_Record_SmartGZIP_Accumulator(){

        double byteSizeLimit = PUT_LIMIT_MB;
        double recordMaxSize = RECORD_COMPRESSED_MAX_SIZE_IN_BYTES;
        PutRecordMode putRecordMode = PutRecordMode.SINGLE;
        List<String> streamNames = Collections.singletonList("FAKE_STREAM");
        FirehoseAccumulatorFactory<String> factory = FirehoseAccumulatorFactory.getSmartGZIPFactory(byteSizeLimit, new BasicFirehoseJsonStringMapper<>());
        TransferQueue<List<Record>> producedRecords = new LinkedTransferQueue<>();


        // BOOSTRAP SERVICE
        FirehoseQIOServiceTest firehoseQIOService = new FirehoseQIOServiceTest(String.class, 2, null, null)
                .withStreamNames(streamNames)
                .withPutRecordMode(putRecordMode)
                .withByteBatchingPerConsumerThread(factory, 10, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        firehoseQIOService.withPutRecordBatchCallback((records, s) -> {
            producedRecords.add(records);
        });


        // START CONSUMING
        firehoseQIOService.startConsuming();


        // GENERATE FAKE DATAS
        this.tenByteStrings(10_000_000)
                .forEach(i -> {
                    firehoseQIOService.getInputQueue().add(i);
                });


        try {
            List<List<Record>> elementsProduced = new ArrayList<>();
            Queues.drain(producedRecords, elementsProduced, 10, 15, TimeUnit.SECONDS);


            // EXPECT THAT EVERY CHUNK HAS A TOTAL SUM OF BYTES < byteSizeLimit
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(s -> s.stream().mapToInt(record -> record.getData().array().length).sum())
                    .filter(sum -> sum < byteSizeLimit)
                    .count() == elementsProduced.size(), "Size > "+byteSizeLimit);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(r -> r.getData().array().length <= recordMaxSize)
                    .count() == elementsProduced.stream().mapToLong(Collection::size).sum(), "Single record have size > "+recordMaxSize);

        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }



    @Test
    public void test_Put_Record_BATCH_Basic_Accumulator(){

        double byteSizeLimit = PUT_BATCH_LIMIT_MB;
        double recordMaxSize = RECORD_MAX_SIZE_IN_BYTES;
        PutRecordMode putRecordMode = PutRecordMode.BATCH;
        List<String> streamNames = Collections.singletonList("FAKE_STREAM");
        FirehoseAccumulatorFactory<String> factory = FirehoseAccumulatorFactory.getBasicRecordFactory(byteSizeLimit, new BasicFirehoseJsonStringMapper<>());
        TransferQueue<List<Record>> producedRecords = new LinkedTransferQueue<>();


        // BOOSTRAP SERVICE
        FirehoseQIOServiceTest firehoseQIOService = new FirehoseQIOServiceTest(String.class, 2, null, null)
                .withStreamNames(streamNames)
                .withPutRecordMode(putRecordMode)
                .withByteBatchingPerConsumerThread(factory, 10, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        firehoseQIOService.withPutRecordBatchCallback((records, s) -> {
            producedRecords.add(records);
        });


        // START CONSUMING
        firehoseQIOService.startConsuming();


        // GENERATE FAKE DATAS
        this.tenByteStrings(10_000_000)
                .forEach(i -> {
                    firehoseQIOService.getInputQueue().add(i);
                });


        try {
            List<List<Record>> elementsProduced = new ArrayList<>();
            Queues.drain(producedRecords, elementsProduced, 10, 15, TimeUnit.SECONDS);


            // EXPECT THAT EVERY CHUNK HAS A TOTAL SUM OF BYTES < byteSizeLimit
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(s -> s.stream().mapToInt(record -> record.getData().array().length).sum())
                    .filter(sum -> sum < byteSizeLimit)
                    .count() == elementsProduced.size(), "Size > "+byteSizeLimit);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(r -> r.getData().array().length <= recordMaxSize)
                    .count() == elementsProduced.stream().mapToLong(Collection::size).sum(), "Single record have size > "+recordMaxSize);

        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }



    @Test
    public void test_Put_Record_Basic_Accumulator(){

        double byteSizeLimit = PUT_LIMIT_MB;
        double recordMaxSize = RECORD_MAX_SIZE_IN_BYTES;
        PutRecordMode putRecordMode = PutRecordMode.SINGLE;
        List<String> streamNames = Collections.singletonList("FAKE_STREAM");
        FirehoseAccumulatorFactory<String> factory = FirehoseAccumulatorFactory.getBasicRecordFactory(byteSizeLimit, new BasicFirehoseJsonStringMapper<>());
        TransferQueue<List<Record>> producedRecords = new LinkedTransferQueue<>();


        // BOOSTRAP SERVICE
        FirehoseQIOServiceTest firehoseQIOService = new FirehoseQIOServiceTest(String.class, 2, null, null)
                .withStreamNames(streamNames)
                .withPutRecordMode(putRecordMode)
                .withByteBatchingPerConsumerThread(factory, 10, TimeUnit.SECONDS);

        // DEFINE CALLBACK TO INTERCEPT PRODUCED OBJECTS
        firehoseQIOService.withPutRecordBatchCallback((records, s) -> {
            producedRecords.add(records);
        });


        // START CONSUMING
        firehoseQIOService.startConsuming();


        // GENERATE FAKE DATAS
        this.tenByteStrings(10_000_000)
                .forEach(i -> {
                    firehoseQIOService.getInputQueue().add(i);
                });


        try {
            List<List<Record>> elementsProduced = new ArrayList<>();
            Queues.drain(producedRecords, elementsProduced, 10, 15, TimeUnit.SECONDS);


            // EXPECT THAT EVERY CHUNK HAS A TOTAL SUM OF BYTES < byteSizeLimit
            Assert.isTrue(elementsProduced
                    .stream()
                    .mapToInt(s -> s.stream().mapToInt(record -> record.getData().array().length).sum())
                    .filter(sum -> sum < byteSizeLimit)
                    .count() == elementsProduced.size(), "Size > "+byteSizeLimit);

            // EXPECT EVERY SINGLE Record HAVE A BYTE SUM < recordMaxSize
            Assert.isTrue(elementsProduced
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(r -> r.getData().array().length <= recordMaxSize)
                    .count() == elementsProduced.stream().mapToLong(Collection::size).sum(), "Single record have size > "+recordMaxSize);

        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.isTrue(false, "Cannot pool from result queue");
        }
    }
}
