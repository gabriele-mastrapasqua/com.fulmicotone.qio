package com.fulmicotone.qio.components.metrics;

import com.fulmicotone.qio.components.metrics.types.*;
import com.fulmicotone.qio.models.QueueIOQ;
import com.fulmicotone.qio.models.QueueIOService;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueIOMetric {


    private MetricProducedElements metricProducedElements;
    private MetricReceivedElements metricReceivedElements;
    private MetricProducedBytes metricProducedBytes;
    private MetricReceivedBytes metricReceivedBytes;
    private MetricInputQueueSize metricInputQueueSize;
    private MetricInternalQueuesAVGSize metricInternalQueuesAVGSize;
    private MetricExecutorQueueSize metricExecutorQueueSize;

    public QueueIOMetric(QueueIOService service)
    {
        String uniqueKey = service.getUniqueKey();

        metricProducedElements = new MetricProducedElements(uniqueKey);
        metricReceivedElements = new MetricReceivedElements(uniqueKey);
        metricProducedBytes = new MetricProducedBytes(uniqueKey);
        metricReceivedBytes = new MetricReceivedBytes(uniqueKey);
        metricInputQueueSize = new MetricInputQueueSize(uniqueKey);
        metricExecutorQueueSize = new MetricExecutorQueueSize(uniqueKey);
        metricInternalQueuesAVGSize = new MetricInternalQueuesAVGSize(uniqueKey);
    }


    public void registerMetrics(String appNameSpace)
    {
        metricProducedElements.register(appNameSpace);
        metricReceivedElements.register(appNameSpace);
        metricProducedBytes.register(appNameSpace);
        metricReceivedBytes.register(appNameSpace);
        metricInputQueueSize.register(appNameSpace);
        metricExecutorQueueSize.register(appNameSpace);
        metricInternalQueuesAVGSize.register(appNameSpace);
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

    public void setMetricInternalQueuesAVGSize(List<QueueIOQ> queues){
        double avg = queues.stream().mapToInt(LinkedBlockingQueue::size).average().orElse(0);
        metricInternalQueuesAVGSize.setValue((int)avg);
    }

    public int getInputQueueSizeValue(){
        return metricInputQueueSize.getValue();
    }

    public int getExecutorQueueSizeValue(){
        return metricExecutorQueueSize.getValue();
    }

    public int getMetricInternalQueuesAVGSize(){
        return metricInternalQueuesAVGSize.getValue();
    }
}
