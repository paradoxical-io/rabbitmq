package io.paradoxical.rabbitmq.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.godaddy.logging.Logger;
import io.paradoxical.rabbitmq.metrics.delegators.DelegatedCounter;
import io.paradoxical.rabbitmq.metrics.delegators.DelegatedMeter;
import io.paradoxical.rabbitmq.metrics.delegators.DelegatedTimer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;
import static com.godaddy.logging.LoggerFactory.getLogger;
import static java.util.stream.Collectors.toList;


/**
 * A manager class to handle multithreaded access for eventable metrics
 * Dynamically adds event types to the MetricRegistry as they come in, as well as handles
 * creating metric sets on the fly using the metric group list.
 * <p/>
 * For example, giving a metric group of ["foo", "foo.bar", "biz"] and an incoming event type of `Data`
 * will generate event metrics of:
 * <p/>
 * className.events <- firehose
 * className.events.Data <- specific subtype
 * className.events.foo
 * className.events.foo.Data
 * className.events.foo.bar
 * className.events.foo.bar.Data
 * className.events.biz
 * className.events.biz.Data
 * <p/>
 * This allows us to do metric queries across all the dimensions
 *
 * @param <T>
 */
public class MetricsEventManager<T> {
    private static final Logger logger = getLogger(MetricsEventManager.class);

    private static final Object lock = new Object();

    private final MetricRegistry metricRegistry;
    private final List<String> metricGroups;
    private final Function<T, Class<?>> classMapper;
    private final MetricsEventManagerOptions<T> options;

    public MetricsEventManager(MetricsEventManagerOptions<T> options) {
        this.options = options;
        this.metricRegistry = options.getMetricRegistry();
        this.metricGroups = options.getMetricGroups() == null ? Collections.emptyList() : options.getMetricGroups();
        this.classMapper = options.getClassMapper() != null ? options.getClassMapper() : this::getDefaultEventClassType;
    }

    private List<String> groupNames(String subName) {
        return metricGroups.stream().map(i -> name(i, subName)).collect(toList());
    }

    /**
     * This should never fail as metrics are just not important enough to fail on runtime exceptions
     *
     * @param fullName
     * @param metric
     */
    private void registerMetric(String fullName, Metric metric) {
        try {
            metricRegistry.register(fullName, metric);
        }
        catch (Throwable ex) {
            logger.warn(ex, "Error registering metric!");

            if (options.isThrowOnError()) {
                throw ex;
            }
        }
    }

    private <M extends Metric> M queryRegistryForNamedMetric(String fullTimerName, Supplier<M> onDefault) {
        if (metricRegistry == null) {
            return onDefault.get();
        }

        if (metricRegistry.getMetrics().containsKey(fullTimerName)) {
            Metric metric = metricRegistry.getMetrics().get(fullTimerName);

            try {
                return ((M) metric);
            }
            catch (Exception ex) {
                logger.with("full-name", fullTimerName)
                      .warn("Metric already found, and is not of the expected type. Metric will not be used!");

                return onDefault.get();
            }
        }

        return null;
    }

    /**
     * Allow subclasses to give a more specific type of what the current event type is.
     * This allows for wrapped event types to show up as the inner type
     *
     * @param item
     * @return
     */
    protected Class<?> getDefaultEventClassType(T item) {
        return item.getClass();
    }

    /**
     * Metrics are logged by event type, so we need to determine
     * the type of the event, or delegate the type location to a subclass.
     * <p/>
     *
     * @param item
     * @return
     */
    public DelegatedTimer getTimerByEventType(final T item, String eventPrefix) {
        try {
            return new DelegatedTimer(getMetricByEventType(item, eventPrefix, Timer::new, Timer.class));
        }
        catch (Exception ex) {
            return new DelegatedTimer(Collections.emptyList());
        }
    }

    public DelegatedCounter getCounterByEventType(final T item, String eventPrefix) {
        try {
            return new DelegatedCounter(getMetricByEventType(item, eventPrefix, Counter::new, Counter.class));
        }
        catch (Exception ex) {
            return new DelegatedCounter(Collections.emptyList());
        }
    }

    /**
     * Gets a unique metric and associates it with the groups if not already
     *
     * @param subName
     * @return
     */
    public DelegatedTimer getTimer(String subName) {
        try {
            final List<Timer> metrics = getMetric(subName, Timer::new, Timer.class);

            return new DelegatedTimer(metrics);
        }
        catch (Throwable ex) {
            logger.warn(ex, "Error creating metric, returning default");

            return new DelegatedTimer(Collections.emptyList());
        }
    }

    public DelegatedCounter getCounter(String subName) {
        try {
            final List<Counter> metrics = getMetric(subName, Counter::new, Counter.class);

            return new DelegatedCounter(metrics);
        }
        catch (Throwable ex) {
            logger.warn(ex, "Error creating metric, returning default");

            return new DelegatedCounter(Collections.emptyList());
        }
    }

    public DelegatedMeter getMeter(String subName) {
        try {
            final List<Meter> metrics = getMetric(subName, Meter::new, Meter.class);

            return new DelegatedMeter(metrics);
        }
        catch (Throwable ex) {
            logger.warn(ex, "Error creating metric, returning default");

            return new DelegatedMeter(Collections.emptyList());
        }
    }

    private <M extends Metric> List<M> getMetric(String subName, Supplier<M> onDefault, Class<M> clazz) {
        M mainMetric = getMetricThreadSafe(subName, onDefault, clazz);

        final List<M> subMetrics =
                groupNames(subName)
                        .stream()
                        .map(name -> getMetricThreadSafe(name, onDefault, clazz))
                        .collect(toList());

        final ArrayList<M> allMetrics = new ArrayList<>(subMetrics);

        allMetrics.add(mainMetric);

        return allMetrics;
    }


    private <M extends Metric> M getMetricThreadSafe(final String subName, final Supplier<M> onDefault, final Class<M> clazz) {
         /*
            Do singleton style testing here. if the named lock already exists
            use that. Otherwise lock, test again, and create if necessary.

            The underling collection in the metrics library is a concurrent map
            so querying is threadsafe
        */

        String fullName = name(options.getSourceId(), subName);

        M metric = queryRegistryForNamedMetric(fullName, onDefault);

        if (metric != null && clazz.isAssignableFrom(metric.getClass())) {
            return metric;
        }

        /*
            Use a static lock to get named instances of timers across the scope of the application.

            This lets us use the same named metric across multiple instances of listeners, which is the scenario
            for a distributed worker queue
         */
        synchronized (lock) {
            metric = queryRegistryForNamedMetric(fullName, onDefault);

            if (metric != null && clazz.isAssignableFrom(metric.getClass())) {
                return metric;
            }

            metric = onDefault.get();

            // the default no groups
            registerMetric(fullName, metric);

            return metric;
        }
    }

    private <M extends Metric> List<M> getMetricByEventType(final T item, final String prefix, final Supplier<M> onDefault, final Class<M> clazz) {
        try {
            Class<?> itemClass;

            try {
                itemClass = classMapper.apply(item);
            }
            catch (Throwable ex) {
                // if the mapper fails here, we shouldn't let that fail
                // the entire event processing for just a metric.

                itemClass = item.getClass();

                logger.error("Error getting event class type, using default", ex);
            }

            return getMetric(prefix + "." + itemClass.getSimpleName(), onDefault, clazz);
        }
        catch (Throwable ex) {
            logger.error(ex, "Error creating metric by event type, returning default");

            return Collections.emptyList();
        }
    }
}
