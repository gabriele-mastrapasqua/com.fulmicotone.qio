package com.fulmicotone.qio.services;


import com.fulmicotone.qio.components.metrics.QueueIOMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class QueueIOStatusService {


    private final Logger log = LoggerFactory.getLogger(getClass());
    private Set<QueueIOService> queueIOServices = new HashSet<>();
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



    public void reset()
    {
        queueIOServices = new HashSet<>();
    }

    public void destroy()
    {
        queueIOServices.forEach(QueueIOService::onDestroy);
    }

    public void registerQueueIOService(QueueIOService queueIO)
    {
        queueIOServices.add(queueIO);
    }

    public void registerMetrics()
    {
        queueIOServices.forEach(q -> q.registerMetrics("QueueIOStatusService"));
    }


    public void updateCloudWatchMetrics() {

        queueIOServices
                .forEach(QueueIOService::updateMetrics);

    }



    public String getMetricsString()
    {
        StringBuilder builder = new StringBuilder();

        header(builder);
        logSystemThreads(builder);

        for(QueueIOService q : queueIOServices.stream()
                .sorted(Comparator.comparing(QueueIOService::getUniqueKey))
                .collect(Collectors.toList()))
        {
            delimiter(q, builder);
            logQIO(q, builder);
        }

        overallFooter(builder);

        return "\n"+builder.toString();
    }


    private void logSystemThreads( StringBuilder builder)
    {
        int totalThreads = 0;
        int activeThreads = 0;
        int systemThreads = Runtime.getRuntime().availableProcessors();

        for(QueueIOService q : queueIOServices.stream()
                .sorted(Comparator.comparing(QueueIOService::getUniqueKey))
                .collect(Collectors.toList()))
        {
            totalThreads += q.singleExecutor.getMaximumPoolSize()+q.multiThreadExecutor.getMaximumPoolSize();
            activeThreads += q.singleExecutor.getActiveCount()+q.multiThreadExecutor.getActiveCount();
            builder.append("QIO_METRIC THREADS - System: "+systemThreads+" - QIOTotal:"+totalThreads+" - QIOActive:"+activeThreads).append("\n");
        }
    }


    private void logQIO(QueueIOService q, StringBuilder builder)
    {
        QueueIOMetric metric = q.getMetrics();
        String qioName = q.getUniqueKey();

        String line10 = "QIO_METRIC "+qioName+ ": Active Threads:  "+q.multiThreadExecutor.getActiveCount()+" - "+q.multiThreadExecutor.getMaximumPoolSize();
        String line1a = "QIO_METRIC "+qioName+ ": InputQueue:        "+metric.getInputQueueSizeValue();
        String line1b = "QIO_METRIC "+qioName+ ": SingleExecQueue:   "+metric.getSingleExecutorQueueSizeValue();
        String line1c = "QIO_METRIC "+qioName+ ": MultiExecQueue:    "+metric.getMultiExecutorQueueSizeValue();
        String line1d = "QIO_METRIC "+qioName+ ": IntervalAVGQueue:  "+metric.getMetricInternalQueuesAVGSize();

        String line2 = "QIO_METRIC "+qioName+
                ": Elements (In/Out): "+
                metric.getMetricReceivedElements().getValue()+" - "+metric.getMetricProducedElements().getValue();

        String line3 = "QIO_METRIC "+qioName+
                ": Bytes (In/Out)     "+
                metric.getMetricReceivedBytes().getValue()+" - "+metric.getMetricProducedBytes().getValue();

        builder.append(String.join("\n", line10, line1a, line1b, line1c, line1d, line2, line3)).append("\n");
    }

    private void delimiter(QueueIOService q, StringBuilder builder)
    {
        final Function<String, String> wrap = s -> "QIO_METRIC \n";
        builder.append(wrap.apply(""));
    }


    private void header(StringBuilder builder)
    {
        final Function<String, String> wrap = date -> "QIO_METRIC --------- "+date+" ----------\n";
        builder.append(wrap.apply(formatter.format(LocalDateTime.now())));
    }

    private void overallFooter(StringBuilder builder)
    {
        builder.append("QIO_METRIC -> ----------------------------------------  \n");
    }

}
