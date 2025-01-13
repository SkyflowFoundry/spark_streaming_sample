package com.skyflow.walmartpoc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

// Abstract Datum class
abstract class Datum {
    protected final String metricName;
    protected final StandardUnit unit;
    protected final List<Dimension> dimensions;

    public Datum(String metricName, Map<String, String> dimensions, StandardUnit unit) {
        this.metricName = metricName;
        this.unit = unit;
        this.dimensions = (dimensions == null ? Arrays.asList() : dimensions.entrySet().stream().map(
            entry -> Dimension.builder()
                              .name(entry.getKey())
                              .value(entry.getValue())
                              .build()
        ).collect(Collectors.toList()));
    }

    public abstract MetricDatum metricDatum();
    public abstract void reset();
}

// ValueDatum: Tracks counts
class ValueDatum extends Datum {
    private long count = 0;

    public ValueDatum(String metricName, Map<String, String> dimensions, StandardUnit unit) {
        super(metricName, dimensions, unit);
    }

    public synchronized void increment() {
        count ++;
    }

    public synchronized void increment(long value) {
        count += value;
    }

    @Override
    public synchronized MetricDatum metricDatum() {
        MetricDatum result = MetricDatum.builder()
                                  .metricName(metricName)
                                  .dimensions(dimensions)
                                  .unit(unit)
                                  .value((double) count)
                                  .build();
        reset();
        return result;
    }

    @Override
    public synchronized void reset() {
        count = 0;
    }
}

// StatisticDatum: Tracks min, max, sum, and count
class StatisticDatum extends Datum {
    private double sum = 0;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private long count = 0;

    public StatisticDatum(String metricName, Map<String, String> dimensions, StandardUnit unit) {
        super(metricName, dimensions, unit);
    }

    public synchronized void value(double value) {
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        count++;
    }

    @Override
    public synchronized MetricDatum metricDatum() {
        if (count == 0) {
            return null; // No data to report
        }
        StatisticSet stats = StatisticSet.builder()
                .sampleCount((double) count)
                .sum(sum)
                .minimum(min)
                .maximum(max)
                .build();

            MetricDatum result = MetricDatum.builder()
                .metricName(metricName)
                .dimensions(dimensions)
                .unit(unit)
                .statisticValues(stats)
                .build();

        reset();
        return result;
    }

    @Override
    public synchronized void reset() {
        sum = 0;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        count = 0;
    }
}
