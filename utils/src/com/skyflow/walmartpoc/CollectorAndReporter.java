package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
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

    private static String serializeDimensions(Map<String, String> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return "";
        }
        return dimensions.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue().replace("\\", "\\\\").replace(",", "\\,"))
                .collect(Collectors.joining(","));
    } 

    private String serializeDimensions(List<Dimension> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return "";
        }
        return dimensions.stream()
                .sorted(Comparator.comparing(Dimension::name))
                .map(dimension -> dimension.name() + "=" + dimension.value().replace("\\", "\\\\").replace(",", "\\,"))
                .collect(Collectors.joining(","));
    }

    public <T extends Datum> T createOrGetUniqueMetric(String metricName, Map<String, String> dimensions, StandardUnit unit, Class<T> datumClass) {
        @SuppressWarnings("unchecked")
        T ret = (T) metrics.computeIfAbsent(String.format("%s %s",metricName,serializeDimensions(dimensions)), name -> {
            if (datumClass==StatisticDatum.class) {
                return new StatisticDatum(metricName, dimensions, unit);
            } else if (datumClass==ValueDatum.class) {
                return new ValueDatum(metricName, dimensions, unit);
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
            if (datum.hasDimensions()) {
                System.out.print("  Dimensions: " + serializeDimensions((datum.dimensions())));
            }
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
