package com.fulmicotone.qio.example;


import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.example.models.PageView;
import com.fulmicotone.qio.example.qio.DomainCountQIO;
import com.fulmicotone.qio.example.qio.IntentDiscoverQIO;
import com.fulmicotone.qio.example.qio.IntentStorerQIO;
import com.fulmicotone.qio.models.OutputQueues;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class GithubExampleSizeAccumulator {





    public static void main(String[] args)
    {

        // INTENT STORER QIO - Will store the Intent objects to the database.
        IntentStorerQIO intentStorerQIO = new IntentStorerQIO(Intent.class,2, null);

        // INTENT DISCOVER QIO - Output queue will be the IntentStorerQIO input queue
        IntentDiscoverQIO intentDiscoverQIO = new IntentDiscoverQIO(PageView.class,4, new OutputQueues()
                .withQueue(Intent.class, intentStorerQIO.getInputQueue()));

        // PAGEVIEW QIO - Output queue will be the DomainCount.class queue.
        // The service will group 5 ingested Pageviews before sending them to ingestionTask method
        DomainCountQIO domainCountQIO = new DomainCountQIO(PageView.class,2, null)
                .withSizeBatchingPerConsumerThread(5, 5, TimeUnit.SECONDS);


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


    }
}
