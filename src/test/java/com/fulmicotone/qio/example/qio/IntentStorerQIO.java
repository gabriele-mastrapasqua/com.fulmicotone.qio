package com.fulmicotone.qio.example.qio;

import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.example.models.PageView;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOService;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class IntentStorerQIO extends QueueIOService<Intent> {

    public IntentStorerQIO(Class<Intent> clazz, OutputQueues outputQueues) {
        super(clazz, outputQueues);
    }

    @Override
    public IQueueIOIngestionTask<Intent> ingestionTask() {
        return new IQueueIOIngestionTask<Intent>() {


            // Simulate call to database
            private void saveToDB(Intent intent)
            {
                System.out.println(Thread.currentThread().getName()+" - Intent stored! Id:"+intent.getIntentId()+" - UserId:"+intent.getUserId());
            }
            
            @Override
            public Void ingest(List<Intent> list) {

                // Save to database
                list.forEach(this::saveToDB);
                return null;
            }
        };
    }
}
