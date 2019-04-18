package com.fulmicotone.qio;

import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.models.QueueIOService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class StringProducerQueueIO extends QueueIOService<String, String> {


    public StringProducerQueueIO(Class<String> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<String, String> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public StringProducerQueueIO(Class<String> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<String, String> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
    }

    @Override
    public IQueueIOIngestionTask<String> ingestionTask() {


        return new IQueueIOIngestionTask<String>() {


            private final Logger log = LoggerFactory.getLogger(getClass());
            int increment = 0;

            @Override
            public Void ingest(List<String> list) {

                list.forEach(elm -> {
                    String str = Thread.currentThread()+"="+increment++;
                    produce(str, String.class);
                });

                return null;
            }
        };
    }
}
