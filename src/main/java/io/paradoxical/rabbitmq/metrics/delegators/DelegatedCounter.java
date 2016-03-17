package io.paradoxical.rabbitmq.metrics.delegators;

import com.codahale.metrics.Counter;

import java.util.List;

public class DelegatedCounter implements DelegatedMetric {
    private final List<Counter> counters;

    public DelegatedCounter(List<Counter> counters) {
        this.counters = counters;
    }

    public void inc() {
        counters.forEach(Counter::inc);
    }

    public void dec() {
        counters.forEach(Counter::dec);
    }
}
