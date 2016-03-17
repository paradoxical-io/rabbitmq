package io.paradoxical.rabbitmq.metrics;

import com.codahale.metrics.MetricRegistry;
import lombok.Data;

import java.util.List;
import java.util.function.Function;

@Data
public class MetricsEventManagerOptions<T> {
    private final MetricRegistry metricRegistry;
    private final String sourceId;
    private final List<String> metricGroups;
    private final Function<T, Class<?>> classMapper;
    private boolean throwOnError = false;

    @java.beans.ConstructorProperties({ "metricRegistry", "sourceId", "metricGroups", "classMapper", "throwOnError" })
    MetricsEventManagerOptions(
            final MetricRegistry metricRegistry,
            final String sourceId,
            final List<String> metricGroups,
            final Function<T, Class<?>> classMapper, final boolean throwOnError) {
        this.metricRegistry = metricRegistry;
        this.sourceId = sourceId;
        this.metricGroups = metricGroups;
        this.classMapper = classMapper;
        this.throwOnError = throwOnError;
    }

    public static <T> MetricsEventManagerOptionsBuilder<T> builder() {return new MetricsEventManagerOptionsBuilder<T>();}

    public static class MetricsEventManagerOptionsBuilder<T> {
        private MetricRegistry metricRegistry;
        private String sourceId;
        private List<String> metricGroups;
        private Function<T, Class<?>> classMapper;
        private boolean throwOnError;

        MetricsEventManagerOptionsBuilder() {}

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> metricRegistry(final MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> sourceClass(final Class sourceClass) {
            this.sourceId = sourceClass.getName();
            return this;
        }

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> sourceId(final String sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> metricGroups(final List<String> metricGroups) {
            this.metricGroups = metricGroups;
            return this;
        }

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> classMapper(final Function<T, Class<?>> classMapper) {
            this.classMapper = classMapper;
            return this;
        }

        public MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder<T> throwOnError(final boolean throwOnError) {
            this.throwOnError = throwOnError;
            return this;
        }

        public MetricsEventManagerOptions<T> build() {return new MetricsEventManagerOptions<T>(metricRegistry, sourceId, metricGroups, classMapper, throwOnError);}

        public String toString() {
            return "com.godaddy.domains.metrics.MetricsEventManagerOptions.MetricsEventManagerOptionsBuilder(metricRegistry=" + this.metricRegistry + ", sourceId=" +
                   this.sourceId + ", metricGroups=" + this.metricGroups + ", classMapper=" + this.classMapper + ", throwOnError=" + this.throwOnError + ")";
        }
    }
}
