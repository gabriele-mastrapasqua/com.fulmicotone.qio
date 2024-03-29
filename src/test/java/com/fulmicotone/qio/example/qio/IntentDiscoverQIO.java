package com.fulmicotone.qio.example.qio;

import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.example.models.PageView;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOService;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class IntentDiscoverQIO extends QueueIOService<PageView, PageView> {


    public IntentDiscoverQIO(Class<PageView> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<PageView, PageView> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public IntentDiscoverQIO(Class<PageView> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<PageView, PageView> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
    }

    @Override
    public IQueueIOIngestionTask<PageView> ingestionTask() {
        return new IQueueIOIngestionTask<PageView>() {

            SplittableRandom random = new SplittableRandom();

            // FAKE CALL to database - simulate both cases (found/not found) with random.
            private Optional<Intent> getIntentFromUserPageView(PageView userPageView)
            {
                int intentId = random.nextInt(0, 10);

                if(intentId % 2 == 0){
                    System.out.println(Thread.currentThread().getName()+" - New intent found for pageview "+userPageView.getUrl()
                            +" - Intent id:"+intentId
                            +" - UserId:"+userPageView.getUserId());
                    return Optional.of(new Intent(userPageView.getUserId(), intentId+""));
                }
                return Optional.empty();
            }
            
            @Override
            public Void ingest(List<PageView> list) {

                // FAKE GET from database
                List<Intent> intents = list.stream()
                        .map(this::getIntentFromUserPageView)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());

                produceAll(intents, Intent.class);

                return null;
            }
        };
    }
}
