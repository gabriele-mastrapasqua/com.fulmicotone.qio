package com.fulmicotone.qio.components.metrics;

import com.fulmicotone.qio.components.metrics.types.*;
import com.fulmicotone.qio.models.QueueIOQ;
import com.fulmicotone.qio.models.QueueIOService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueueIOMetric {


    private MetricProducedElements metricProducedElements;
    private MetricReceivedElements metricReceivedElements;
    private MetricProducedBytes metricProducedBytes;
    private MetricReceivedBytes metricReceivedBytes;
    private MetricInputQueueSize metricInputQueueSize;
    private List<MetricInternalQueueSize> metricInternalQueueSize;
    private MetricExecutorQueueSize metricExecutorQueueSize;
    private QueueIOService service;

    public QueueIOMetric(QueueIOService service)
    {
        String uniqueKey = service.getUniqueKey();

        metricProducedElements = new MetricProducedElements(uniqueKey);
        metricReceivedElements = new MetricReceivedElements(uniqueKey);
        metricProducedBytes = new MetricProducedBytes(uniqueKey);
        metricReceivedBytes = new MetricReceivedBytes(uniqueKey);
        metricInputQueueSize = new MetricInputQueueSize(uniqueKey);
        metricExecutorQueueSize = new MetricExecutorQueueSize(uniqueKey);
        metricInternalQueueSize = new ArrayList<>();

        IntStream.range(0, service.getInternalThreads()).forEach(i -> metricInternalQueueSize.add(new MetricInternalQueueSize(uniqueKey, i)));
    }


    public void registerMetrics(String appNameSpace)
    {
        metricProducedElements.register(appNameSpace);
        metricReceivedElements.register(appNameSpace);
        metricProducedBytes.register(appNameSpace);
        metricReceivedBytes.register(appNameSpace);
        metricInputQueueSize.register(appNameSpace);
        metricExecutorQueueSize.register(appNameSpace);
        metricInternalQueueSize.forEach(m -> m.register(appNameSpace));
    }


    public MetricProducedElements getMetricProducedElements() {
        return metricProducedElements;
    }

    public MetricReceivedElements getMetricReceivedElements() {
        return metricReceivedElements;
    }

    public MetricProducedBytes getMetricProducedBytes() {
        return metricProducedBytes;
    }

    public MetricReceivedBytes getMetricReceivedBytes() {
        return metricReceivedBytes;
    }

    public void setMetricInputQueueSizeValue(BlockingQueue queue){
        metricInputQueueSize.setValue(queue.size());
    }

    public void setMetricExecutorQueueSize(BlockingQueue queue){
        metricExecutorQueueSize.setValue(queue.size());
    }

    public void setMetricInternalQueueSize(List<QueueIOQ> queues){
        for(int i=0; i< queues.size(); i++){
            if(metricInternalQueueSize.get(i) != null){
                metricInternalQueueSize.get(i).setValue(queues.get(i).size());
            }
        }
    }

    public int getInputQueueSizeValue(){
        return metricInputQueueSize.getValue();
    }

    public int getExecutorQueueSizeValue(){
        return metricExecutorQueueSize.getValue();
    }

    public List<Integer> getMetricInternalQueueSize(){
        return metricInternalQueueSize
                .stream()
                .sorted((o1, o2) -> (o1.getValue() < o2.getValue()) ? -1 : (o1.getValue().equals(o2.getValue())) ? 0 : 1)
                .map(MetricInternalQueueSize::getValue).collect(Collectors.toList());
    }
}
