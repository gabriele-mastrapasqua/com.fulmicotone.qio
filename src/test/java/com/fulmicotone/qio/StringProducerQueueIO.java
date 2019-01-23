package com.fulmicotone.qio;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StringProducerQueueIO extends QueueIOService<String> {


    public StringProducerQueueIO(Class<String> clazz, OutputQueues outputQueues) {
        super(clazz, outputQueues);
    }

    @Override
    public IQueueIOIngestionTask<String> ingestionTask() {


        return new IQueueIOIngestionTask<String>() {

            private final Logger log = LoggerFactory.getLogger(getClass());
            int increment = 0;

            @Override
            public Void ingest(List<String> list) {

                list.forEach(elm -> {
                    String str = Thread.currentThread().getName()+"="+increment++;
                    produce(str, String.class);
                });

                return null;
            }
        };
    }
}
