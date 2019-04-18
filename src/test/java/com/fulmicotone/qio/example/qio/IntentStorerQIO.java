package com.fulmicotone.qio.example.qio;

import com.fulmicotone.qio.example.models.Intent;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOService;

import java.util.List;

public class IntentStorerQIO extends QueueIOService<Intent, Intent> {


    public IntentStorerQIO(Class<Intent> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<Intent, Intent> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public IntentStorerQIO(Class<Intent> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<Intent, Intent> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
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
