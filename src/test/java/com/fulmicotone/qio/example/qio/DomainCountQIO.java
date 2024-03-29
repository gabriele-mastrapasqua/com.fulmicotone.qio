package com.fulmicotone.qio.example.qio;

import com.fulmicotone.qio.example.models.DomainCount;
import com.fulmicotone.qio.example.models.PageView;
import com.fulmicotone.qio.example.utils.DomainExtractor;
import com.fulmicotone.qio.interfaces.IQueueIOIngestionTask;
import com.fulmicotone.qio.interfaces.IQueueIOTransform;
import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.services.QueueIOService;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DomainCountQIO extends QueueIOService<PageView, PageView> {


    public DomainCountQIO(Class<PageView> clazz, Integer threadSize, OutputQueues outputQueues, IQueueIOTransform<PageView, PageView> transformFunction) {
        super(clazz, threadSize, outputQueues, transformFunction);
    }

    public DomainCountQIO(Class<PageView> clazz, Integer threadSize, Integer multiThreadQueueSize, OutputQueues outputQueues, IQueueIOTransform<PageView, PageView> transformFunction) {
        super(clazz, threadSize, multiThreadQueueSize, outputQueues, transformFunction);
    }

    @Override
    public IQueueIOIngestionTask<PageView> ingestionTask() {
        return new IQueueIOIngestionTask<PageView>() {
            
            final DomainExtractor domainExtractor = new DomainExtractor();
            
            @Override
            public Void ingest(List<PageView> list) {

                System.out.println("Received "+list.size()+" pageViews");

                // GROUP BY DOMAIN and SUM
                list.stream()
                        .map(pv -> domainExtractor.apply(pv.getUrl()))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.groupingBy(d -> d, Collectors.counting()))
                        .entrySet()
                        .stream()
                        .map(e -> new DomainCount(e.getKey(), e.getValue()))
                        .forEach(e -> System.out.println(Thread.currentThread().getName()+" - Domain counter: "+e.getDomain()+" occurrences:"+e.getCount()));

                return null;
            }
        };
    }
}
