package io.paradoxical.rabbitmq;

import com.codahale.metrics.MetricRegistry;
import lombok.Data;

import java.util.Arrays;
import java.util.List;


@Data
public class ListenerOptions {

    private final QueueSerializer serializer;

    private final MetricRegistry metricRegistry;

    private final List<String> metricGroups;

    private final Integer maxRetries;

    public ListenerOptions(
            final QueueSerializer serializer,
            final MetricRegistry metricRegistry,
            final List<String> metricGroups,
            final Integer maxRetries) {

        this.serializer = serializer != null ? serializer : new DefaultSerializer();

        this.metricRegistry = metricRegistry;

        this.metricGroups = metricGroups;

        this.maxRetries = maxRetries == null ? 1 : maxRetries;
    }

    public static final ListenerOptions Default = builder().build();

    public static ListenerOptionsBuilder builder() {return new ListenerOptionsBuilder();}

    public static class ListenerOptionsBuilder {
        private QueueSerializer serializer;
        private MetricRegistry metricRegistry;
        private List<String> metricGroups;
        private Integer maxRetries;

        ListenerOptionsBuilder() {}

        public ListenerOptions.ListenerOptionsBuilder serializer(final QueueSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public ListenerOptions.ListenerOptionsBuilder metricRegistry(final MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public ListenerOptions.ListenerOptionsBuilder maxRetries(final Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public ListenerOptions.ListenerOptionsBuilder metricGroups(final String... metricGroups) {
            return metricGroups(Arrays.asList(metricGroups));
        }

        public ListenerOptions.ListenerOptionsBuilder metricGroups(final List<String> metricGroups) {
            this.metricGroups = metricGroups;
            return this;
        }

        public ListenerOptions build() {
            return new ListenerOptions(serializer, metricRegistry, metricGroups, maxRetries);
        }

    }
}
