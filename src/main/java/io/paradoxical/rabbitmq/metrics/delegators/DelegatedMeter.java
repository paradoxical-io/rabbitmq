package io.paradoxical.rabbitmq.metrics.delegators;

import com.codahale.metrics.Meter;

import java.util.List;

public class DelegatedMeter implements DelegatedMetric {
    private final List<Meter> metrics;

    public DelegatedMeter(final List<Meter> metrics) {
        this.metrics = metrics;
    }

    public void mark() {
        metrics.forEach(Meter::mark);
    }
}
