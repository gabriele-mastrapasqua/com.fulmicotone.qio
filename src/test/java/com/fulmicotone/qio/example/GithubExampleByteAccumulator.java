package com.fulmicotone.qio.example;


import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorFactory;
import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.example.models.PageView;
import com.fulmicotone.qio.example.qio.DomainCountQIO;
import com.fulmicotone.qio.example.qio.IntentDiscoverQIO;
import com.fulmicotone.qio.example.qio.IntentStorerQIO;
import com.fulmicotone.qio.example.utils.PageViewCSVAccumulator;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOServiceObserver;
import com.fulmicotone.qio.services.QueueIOStatusService;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class GithubExampleByteAccumulator {





    public static void main(String[] args)
    {
        // OBSERVER
        QueueIOStatusService statusService = new QueueIOStatusService();
        QueueIOServiceObserver serviceObserver = new QueueIOServiceObserver()
                .withStatusService(statusService);



        // INTENT STORER QIO - Will store the Intent objects to the database.
        IntentStorerQIO intentStorerQIO = new IntentStorerQIO(Intent.class,2, null, t -> t);

        // INTENT DISCOVER QIO - Output queue will be the IntentStorerQIO input queue
        IntentDiscoverQIO intentDiscoverQIO = new IntentDiscoverQIO(PageView.class, 4,new OutputQueues()
                .withQueue(Intent.class, intentStorerQIO.getInputQueue()), t -> t);

        // Accumulator of 100 bytes of data
        IQueueIOAccumulatorFactory<PageView, PageView> PageViewCSVAccumulatorFactory = () -> new PageViewCSVAccumulator(100);

        // PAGEVIEW QIO - Output queue will be the DomainCount.class queue.
        // The service will group 100kb of PageView (4 objs) before sending them to ingestionTask method
        DomainCountQIO domainCountQIO = new DomainCountQIO(PageView.class,2, null, t -> t)
                .withByteBatchingPerConsumerThread(PageViewCSVAccumulatorFactory, 5, TimeUnit.SECONDS);

        // REGISTER
        serviceObserver.registerObject(intentDiscoverQIO);
        serviceObserver.registerObject(intentStorerQIO);
        serviceObserver.registerObject(domainCountQIO);

        // START CONSUMING
        intentStorerQIO.startConsuming();
        domainCountQIO.startConsuming();
        intentDiscoverQIO.startConsuming();



        // GENERATE FAKE DATAS
        IntStream.range(0, 10)
                .forEach(i -> {
                    PageView pv = new PageView("http://www.google.it", "uid"+i);
                    domainCountQIO.getInputQueue().add(pv);
                    intentDiscoverQIO.getInputQueue().add(pv);
                });


        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(statusService.getMetricsString());

    }
}
