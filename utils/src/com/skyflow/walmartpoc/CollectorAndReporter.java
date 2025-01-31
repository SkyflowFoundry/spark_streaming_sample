package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

public class CollectorAndReporter implements AutoCloseable {
    private final CloudWatchClient cloudWatch;
    private final ConcurrentMap<String, Datum> metrics = new ConcurrentHashMap<>();
    private final String namespace;
    private long lastReportTime;
    private final long configuredIntervalMs;
            
    public CollectorAndReporter(String namespace, long periodInMillisecs) {
        if (namespace == null || namespace.isEmpty() || "(null)".equals(namespace) || "(console)".equals(namespace)) {
            this.cloudWatch = null;
        } else {
            this.cloudWatch = CloudWatchClient.create();
        }
        this.namespace = namespace;
        this.lastReportTime = System.currentTimeMillis();
        this.configuredIntervalMs = periodInMillisecs;
    }

    public <T extends Datum> T createOrGetUniqueMetricForName(String metricName, Map<String, String> dimensions, StandardUnit unit, Class<T> datumClass) {
        @SuppressWarnings("unchecked")
        T ret = (T) metrics.computeIfAbsent(metricName, name -> {
            if (datumClass==StatisticDatum.class) {
                return new StatisticDatum(name, dimensions, unit);
            } else if (datumClass==ValueDatum.class) {
                return new ValueDatum(name, dimensions, unit);
            } else {
                throw new RuntimeException("Generator not implemented for " + datumClass);
            }
        });
        return ret;
    }

    public synchronized void pollAndReport(boolean force) {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - lastReportTime;
        if (force || elapsedTime >= configuredIntervalMs) {
            List<MetricDatum> metricDataList = metrics.values().stream()
                .map(Datum::metricDatum)
                .filter(metricDatum -> metricDatum != null)
                .collect(Collectors.toList());
            if (this.cloudWatch==null) {
                sendToConsole(metricDataList);
            } else {
                sendToCloudWatch(metricDataList);
            }
            lastReportTime = currentTime;
        }
    }

    private void sendToConsole(List<MetricDatum> metricDataList) {
        for (MetricDatum datum : metricDataList) {
            System.out.print("Metric: " + datum.metricName());
            System.out.print("  Value: " + datum.value());
            System.out.println("  Unit: " + datum.unit());
            if (datum.statisticValues() != null) {
                StatisticSet s = datum.statisticValues();
                System.out.print("Statistic Set: ");
                System.out.print("  Count: " + s.sampleCount());
                System.out.print("  Avg: " + (s.sum()/s.sampleCount()));
                System.out.print("  Min: " + s.minimum());
                System.out.println("  Max: " + s.maximum());
            }
            System.out.println("---------------------------");
        }
    }

    private void sendToCloudWatch(List<MetricDatum> metricDataList) {
        if (!metricDataList.isEmpty()) {
            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(metricDataList)
                    .build();
            PutMetricDataResponse response =  cloudWatch.putMetricData(request);
            if (response != null) {
                try {
                    if (!response.sdkHttpResponse().isSuccessful()) {
                        System.err.println("Failed to send metrics to CloudWatch. Status code: " + response.sdkHttpResponse().statusCode());
                    }
                } catch (Exception e) {
                    System.err.println("Exception while processing CloudWatch response: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        pollAndReport(true);
        // XXX object is still usable!!
    }
}
