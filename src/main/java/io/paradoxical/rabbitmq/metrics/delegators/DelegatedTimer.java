package io.paradoxical.rabbitmq.metrics.delegators;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class DelegatedTimer implements DelegatedMetric {
    private final List<Timer> allMetrics;

    public <M extends Metric> DelegatedTimer(final List<Timer> allMetrics) {
        this.allMetrics = allMetrics;
    }

    public Context time() {
        return new Context();
    }

    public class Context implements AutoCloseable {
        final List<Timer.Context> timers;

        private Context() {
            timers = allMetrics.stream().map(Timer::time).collect(toList());
        }

        @Override
        public void close() throws Exception {
            stop();
        }

        public void stop() {
            timers.forEach(Timer.Context::close);
        }
    }
}
