package io.paradoxical.rabbitmq.tests;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import io.paradoxical.rabbitmq.tests.data.CachingDataListener;
import io.paradoxical.rabbitmq.tests.data.Data;
import io.paradoxical.rabbitmq.tests.data.DataListener;
import io.paradoxical.rabbitmq.tests.data.DataListenerAsync;
import io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException;
import io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack;
import io.paradoxical.rabbitmq.tests.data.DataListenerRetriable;
import io.paradoxical.rabbitmq.tests.data.Subclass;
import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import io.paradoxical.rabbitmq.*;
import io.paradoxical.rabbitmq.connectionManagment.ChannelOptions;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.CallbackBasedQueueConsumer;
import io.paradoxical.rabbitmq.queues.EventBase;
import io.paradoxical.rabbitmq.queues.MessagePromise;
import io.paradoxical.rabbitmq.queues.PromiseQueueConsumer;
import io.paradoxical.rabbitmq.queues.QueuePublisher;
import io.paradoxical.rabbitmq.results.MessageResult;
import lombok.val;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedFunction;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestQueueListener extends TestBase {


    private static final Logger logger = LoggerFactory.getLogger(TestQueueListener.class);

    private Publisher<Data> publisher;

    @Test
    public void testAsyncListener() throws
                                    KeyManagementException,
                                    NoSuchAlgorithmException,
                                    IOException,
                                    URISyntaxException, InterruptedException {
        String queue = "test.SingleListenerAsyc";

        Exchange exchange = new Exchange(queue);

        PublisherExchange publisherExchange = PublisherExchange.valueOf(exchange);

        publisher = new QueuePublisher<>(getTestChannelProvider(), publisherExchange, UUID::randomUUID);

        DataListenerAsync listener = new DataListenerAsync(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(d, listener.getItem());

        cleanup(publisherExchange.getExchange());
    }

    @Test
    public void test_blocking_queue() throws InterruptedException, KeyManagementException, URISyntaxException, NoSuchAlgorithmException, IOException, TimeoutException {
        String queueName = "test_blocking_queue";

        Exchange exchange = new Exchange(queueName);

        exchange.getOptions().setAutoDelete(true);

        Queue queue = Queue.valueOf(queueName);

        queue.getOptions().setAutoDelete(true);


        CallbackBasedQueueConsumer<Data> dataCallbackBasedConsumer;
        dataCallbackBasedConsumer = new CallbackBasedQueueConsumer<>(getTestChannelProvider(),
                                                                     new SingleQueueConfiguration(exchange, queue),
                                                                     Data.class);

        dataCallbackBasedConsumer.start();

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(exchange), UUID::randomUUID);

        int publishNum = 3;

        for (int i = 0; i < publishNum; i++) {
            publisher.publish(new Data(i));
        }

        final int[] count = { 0 };

        EventProcessor<Data> processor = d -> {
            assertThat(d).isNotNull();

            count[0]++;

            return MessageResult.Ack;
        };

        for (int i = 0; i < publishNum; i++) {
            dataCallbackBasedConsumer.pullNextBlockingRmqMessage(Duration.ofMillis(200), processor);
        }

        dataCallbackBasedConsumer.pullNextBlockingRmqMessage(Duration.ofMillis(200), processor);

        dataCallbackBasedConsumer.stop();

        assertThat(count[0]).isEqualTo(publishNum);
    }

    @Test
    public void test_promise_blocking_queue()
            throws InterruptedException, KeyManagementException, URISyntaxException, NoSuchAlgorithmException, IOException, TimeoutException {
        String queueName = "test_promise_blocking_queue";

        Exchange exchange = new Exchange(queueName);

        exchange.getOptions().setAutoDelete(true);

        Queue queue = Queue.valueOf(queueName);

        queue.getOptions().setAutoDelete(true);


        PromiseQueueConsumer<Data> dataBlockingConsumerSync =
                new PromiseQueueConsumer<>(getTestChannelProvider(),
                                           new SingleQueueConfiguration(exchange, queue),
                                           Data.class);

        dataBlockingConsumerSync.start();

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(exchange), UUID::randomUUID);

        int publishNum = 4;

        final Stream<Data> dataStream = IntStream.range(0, publishNum).mapToObj(Data::new);
        List<Data> data = dataStream.collect(toList());

        for (Data datum : data) {
            publisher.publish(datum);
        }

        final List<MessagePromise<Data>> messageOptions =
                IntStream.range(0, publishNum + 1)
                         .mapToObj(num -> dataBlockingConsumerSync.getNextMessage(Duration.ofMillis(200)))
                         .collect(toList());

        final List<Optional<Message<Data>>> messages = messageOptions.stream()
                                                                     .map(Unchecked.function(MessagePromise::getMessage))
                                                                     .filter(Optional::isPresent)
                                                                     .collect(toList());

        final long promisesWithoutMessages = messageOptions.stream()
                                                           .map(Unchecked.function(MessagePromise::getMessage))
                                                           .filter(o -> !o.isPresent())
                                                           .count();

        messageOptions.stream().forEach(p -> p.complete(MessageResult.Ack));

        dataBlockingConsumerSync.stop();

        assertThat(promisesWithoutMessages).isEqualTo(1);
        assertThat(messages.size()).isEqualTo(publishNum);
    }

    @Test
    public void test_promise_blocking_queue_no_messages()
            throws InterruptedException, KeyManagementException, URISyntaxException, NoSuchAlgorithmException, IOException, ExecutionException, TimeoutException {
        String queueName = "test_promise_blocking_queue";

        Exchange exchange = new Exchange(queueName);

        exchange.getOptions().setAutoDelete(true);

        Queue queue = Queue.valueOf(queueName);

        queue.getOptions().setAutoDelete(true);


        PromiseQueueConsumer<Data> dataBlockingConsumerSync = new PromiseQueueConsumer<>(getTestChannelProvider(),
                                                                                         new SingleQueueConfiguration(exchange, queue),
                                                                                         Data.class);

        dataBlockingConsumerSync.start();

        final int[] count = { 0 };

        for (int i = 0; i < 10 + 1; i++) {
            final MessagePromise<Data> messagePromise = dataBlockingConsumerSync.getNextMessage(Duration.ofMillis(200));

            final Optional<Message<Data>> nextMessage = messagePromise.getMessage();

            nextMessage.ifPresent(m -> count[0]++);
            nextMessage.ifPresent(m -> messagePromise.complete(MessageResult.Ack));
        }

        dataBlockingConsumerSync.stop();

        assertThat(count[0]).isEqualTo(0);
    }

    @Test
    public void test_publish_to_invalid_exchange_cuases_connection_failure()
            throws URISyntaxException, IOException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {
        String queue = "test.test_publish_to_invalid_exchange_cuases_connection_failure_" + new Random().nextInt();

        ChannelProvider testChannelProvider = getTestChannelProvider();

        Exchange exchange = new Exchange(queue);

        publisher = new QueuePublisher<>(testChannelProvider, PublisherExchange.valueOf(exchange), UUID::randomUUID);

        QueueConfiguration listenerInfo = autoDelete(new QueueConfiguration(exchange));

        try {
            publisher.publish(new Data());

            DataListener dataListener = new DataListener(testChannelProvider, listenerInfo);

            dataListener.start();

            // should not throw

            cleanup(exchange);
            cleanup(Queue.valueOf(queue));
        }
        catch (Exception ex) {
            fail("Got io exception publishing to exchange");
        }
    }

    @Test
    public void testDlq() throws Throwable {
        String queue = getUnique("test.dlq");

        ChannelProvider testChannelProvider = getTestChannelProvider();

        Exchange dlqExchange = Exchange.asDlq(getUnique("test.dlq.dlq"));

        dlqExchange.setDeclareQueueWithSameName(true);

        dlqExchange.getDefaultQueue().getOptions().setAutoDelete(true);

        Exchange exchange = new Exchange(queue).withDlq(dlqExchange);

        publisher = new QueuePublisher<>(testChannelProvider, PublisherExchange.valueOf(exchange), UUID::randomUUID);

        QueueConfiguration listenerInfo = autoDelete(new QueueConfiguration(exchange));

        QueueConfiguration dlqListenerInfo = autoDelete(new QueueConfiguration(dlqExchange, dlqExchange.getDefaultQueue()));

        DataListenerFailureByRuntimeException failingListener = new DataListenerFailureByRuntimeException(getTestChannelProvider(), listenerInfo);

        DataListenerAsync dlqListener = new DataListenerAsync(getTestChannelProvider(), dlqListenerInfo);

        failingListener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(1000);

        // start the listener after DLQ's have occurred. this means the queue has to have been created
        // and we can now pick up a message
        dlqListener.start();

        Thread.sleep(500);

        Data item = dlqListener.getItem();

        assertEquals(d, item);

        cleanup(Queue.valueOf(queue));
        cleanup(exchange);
        cleanup(exchange.getDlq().get());
        dlqListenerInfo.getQueues().forEach(this::cleanup);
    }

    @Test
    public void test_exponential_retry() throws InterruptedException, KeyManagementException, URISyntaxException, NoSuchAlgorithmException, IOException {

        Exchange exchange = new Exchange(Exchange.Type.Topic, getUnique("exponential_retry_exchange"))
                .withRetryExchange((attempts, item) -> Optional.of(Duration.ofSeconds(attempts * 2)));

        QueueConfiguration queueConfiguration = autoDelete(new QueueConfiguration(exchange,
                                                                                  Queue.valueOf("worker-queue").withRoute(RoutingConstants.ONE_OR_MORE_SUB_STRING)));

        queueConfiguration.getQueues().forEach((queue) -> {
            cleanup(queue);
            cleanup(queue.getRetryQueue());
        });

        ListenerOptions options = ListenerOptions.builder()
                                                 .maxRetries(3)
                                                 .build();

        ChannelProvider testChannelProvider = getTestChannelProvider(ChannelOptions.builder().prefetchCount(1).build());

        DataListenerRetriable dataListenerRetriable = new DataListenerRetriable(testChannelProvider, queueConfiguration, options);
        DataListenerRetriable dataListenerRetriable2 = new DataListenerRetriable(testChannelProvider, queueConfiguration, options);

        dataListenerRetriable.start();
        dataListenerRetriable2.start();

        val publisher = new PublisherProviderImpl<>(getTestChannelProvider()).forExchange(exchange).onRoute("foo");

        publisher.publish(fixture.manufacturePojo(Data.class));

        Thread.sleep(10 * 1000);

        assertThat(dataListenerRetriable.getCount() + dataListenerRetriable2.getCount()).isEqualTo(3);

        cleanup(exchange);
        cleanup(exchange.getRetryExchange().get());
    }

    private String getUnique(String name) {
        return String.format("%s-%s", name, String.valueOf(new Random().nextInt()));
    }

    @Test
    public void testMultipleListeners() throws
                                        InterruptedException,
                                        KeyManagementException,
                                        URISyntaxException,
                                        NoSuchAlgorithmException,
                                        IOException {
        String queue = "test.MultipleListeners";

        Exchange exchange = new Exchange(queue);

        PublisherExchange publisherExchange = PublisherExchange.valueOf(exchange);

        publisher = new QueuePublisher<>(getTestChannelProvider(), publisherExchange, UUID::randomUUID);

        final int maxPublishedItems = 10;

        final List<DataListener> listeners = IntStream.range(0, 20).mapToObj(i -> {
            try {
                return new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange)));
            }
            catch (Exception ex) {
                return null;
            }
        }).collect(toList());

        listeners.stream().forEach(DataListener::start);

        IntStream.range(0, maxPublishedItems).forEach(i -> {
            Data publishedItem = fixture.manufacturePojo(Data.class);

            try {
                publisher.publish(publishedItem);
            }
            catch (IOException e) {
                logger.error(e, "Error publishing");
            }
        });

        Thread.sleep(500);

        final long count = listeners.stream().map(DataListener::getItem).filter(i -> i != null).count();

        assertEquals(maxPublishedItems, count);

        cleanup(Queue.valueOf(queue));
        cleanup(publisherExchange.getExchange());
    }

    @Test
    public void test_reject_metrics_runtime_exception() throws URISyntaxException, IOException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {
        final List<String> metricGroups = Arrays.asList("registry", "registrar", "registry.registrar");

        final Exchange directExchange = new Exchange("test_reject_metrics_runtime_exception");

        String metricsText = metrics_test_util(i -> new DataListenerFailureByRuntimeException(getTestChannelProvider(),
                                                                                              autoDelete(new QueueConfiguration(directExchange)),
                                                                                              i),
                                               directExchange,
                                               metricGroups,
                                               3,
                                               10);

        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.retries\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.retries.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registrar.retries.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.retries\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.retries.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.registrar.retries.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.retries\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.retries.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.registry.retries.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.retries\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.retries.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureByRuntimeException.retries.Subclass\n" +
                                         "             count = 5");

        assertThat(metricsText).contains(Data.class.getSimpleName());
        assertThat(metricsText).contains(Subclass.class.getSimpleName());
    }

    @Test
    public void test_reject_metrics_expected_nack() throws URISyntaxException, IOException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {
        final List<String> metricGroups = Arrays.asList("registry", "registrar", "registry.registrar");

        final Exchange directExchange = new Exchange("test_reject_metrics_expected_nack");

        cleanup(directExchange);

        String metricsText = metrics_test_util(i -> new DataListenerFailureNack(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)), i),
                                               directExchange,
                                               metricGroups,
                                               3,
                                               10);

        assertThat(metricsText).doesNotContain("io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.retries\n");

        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registrar.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.inflight\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.inflight.Data\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.inflight.Subclass\n" +
                                         "             count = 0\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.registrar.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.registry.reject.Subclass\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.reject\n" +
                                         "             count = 10\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.reject.Data\n" +
                                         "             count = 5\n" +
                                         "io.paradoxical.rabbitmq.tests.data.DataListenerFailureNack.reject.Subclass\n" +
                                         "             count = 5");

        assertThat(metricsText).contains(Data.class.getSimpleName());
        assertThat(metricsText).contains(Subclass.class.getSimpleName());
    }

    @Test
    public void testConsoleStats() throws URISyntaxException, IOException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {

        final List<String> metricGroups = Arrays.asList("registry", "registrar", "registry.registrar");

        final Exchange directExchange = new Exchange("testConsoleStats");

        directExchange.getOptions().setAutoDelete(true);

        String metricsText = metrics_test_util(i -> new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)), i),
                                               directExchange,
                                               metricGroups,
                                               3,
                                               10);

        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registry.events\n");
        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registrar.events\n");
        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registry.registrar.events\n");
        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registry.registrar.events\n");
        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registry.registrar.events.Subclass\n");
        assertThat(metricsText).contains("io.paradoxical.rabbitmq.tests.data.DataListener.registry.registrar.events.Data\n");
        assertThat(metricsText).contains(Data.class.getSimpleName());
        assertThat(metricsText).contains(Subclass.class.getSimpleName());
    }

    private <T extends EventBase> String metrics_test_util(
            CheckedFunction<ListenerOptions, ListenerBase<T>> listener,
            Exchange exchange,
            List<String> metricGroups,
            int numConnections,
            int numMessages) throws URISyntaxException, IOException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {
        MetricRegistry registry = new MetricRegistry();

        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

        PrintStream printStream = new PrintStream(byteBuffer);

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                                        .convertRatesTo(TimeUnit.SECONDS)
                                                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                        .outputTo(printStream)
                                                        .build();

        ListenerOptions listenerOptions = ListenerOptions.builder()
                                                         .metricRegistry(registry)
                                                         .metricGroups(metricGroups)
                                                         .build();

        List<ListenerBase<T>> listeners = IntStream.range(0, numConnections).mapToObj(i -> {
            try {
                return listener.apply(listenerOptions);
            }
            catch (Throwable throwable) {
                logger.error(throwable, "Error");

                return null;
            }
        }).collect(toList());

        listeners.forEach(ListenerBase::start);

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(exchange), UUID::randomUUID);

        IntStream.range(0, numMessages).forEach(i -> {
            try {
                Data d = i % 2 == 0 ? fixture.manufacturePojo(Data.class) : fixture.manufacturePojo(Subclass.class);

                publisher.publish(d);
            }
            catch (Throwable e) {
                logger.error(e, "Error");
            }
        });

        Thread.sleep(500);

        cleanup(exchange);

        reporter.report();

        return new String(byteBuffer.toByteArray());
    }

    @Test
    public void testSingleListener() throws
                                     IOException,
                                     InterruptedException,
                                     NoSuchAlgorithmException,
                                     KeyManagementException,
                                     URISyntaxException {

        final Exchange directExchange = new Exchange("testSingleListener");

        directExchange.getOptions().setAutoDelete(true);

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(directExchange), () -> UUID.randomUUID());

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(d, listener.getItem());

    }

    @Test
    public void testJsonPolymorphism() throws
                                       IOException,
                                       InterruptedException,
                                       NoSuchAlgorithmException,
                                       KeyManagementException,
                                       URISyntaxException {

        final Exchange directExchange = new Exchange("testJsonPolymorphism");

        directExchange.getOptions().setAutoDelete(true);

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(directExchange), UUID::randomUUID);

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)));

        listener.start();

        Subclass d = fixture.manufacturePojo(Subclass.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(d, listener.getItem());
    }

    @Test
    public void testSingleListenerWithNullCorrIdProvider() throws
                                                           IOException,
                                                           InterruptedException,
                                                           NoSuchAlgorithmException,
                                                           KeyManagementException,
                                                           URISyntaxException {

        final Exchange directExchange = new Exchange("testSingleListenerWithNullCorrIdProvider");

        directExchange.getOptions().setAutoDelete(true);

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(directExchange), null);

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(d, listener.getItem());
    }

    @Test
    public void testSingleListenerWithCorrId() throws
                                               IOException,
                                               InterruptedException,
                                               NoSuchAlgorithmException,
                                               KeyManagementException,
                                               URISyntaxException {

        final Exchange directExchange = new Exchange("testSingleListenerWithCorrId");

        directExchange.getOptions().setAutoDelete(true);

        cleanup(directExchange);

        UUID corrId = UUID.randomUUID();

        publisher = new QueuePublisher<>(getTestChannelProvider(), PublisherExchange.valueOf(directExchange), () -> corrId);

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(directExchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        d.setCorrelationId(null);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(d, listener.getItem());
        assertEquals(listener.getItem().getCorrelationId(), corrId);
    }

    @Test
    public void testFailureRetriesOnce() throws
                                         IOException,
                                         InterruptedException,
                                         NoSuchAlgorithmException,
                                         KeyManagementException,
                                         URISyntaxException {

        String queue = "test.ExpectingFailureToDlq";

        final Exchange exchange = new Exchange(queue);

        exchange.getOptions().setAutoDelete(true);

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        publisher = publisherProvider.toQueue(exchange);

        DataListenerFailureByRuntimeException listener = new DataListenerFailureByRuntimeException(getTestChannelProvider(), autoDelete(new QueueConfiguration
                                                                                                                                                (exchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertEquals(2, listener.getCount());

        cleanup(Queue.valueOf(queue));
        cleanup(exchange);
    }

    @Test
    public void test_routing_keys() throws
                                    IOException,
                                    InterruptedException,
                                    NoSuchAlgorithmException,
                                    KeyManagementException,
                                    URISyntaxException {

        final Queue queue = autoDelete(new Queue("consumer.test_routing_keys", Optional.of(Arrays.asList("foo.*.bar"))));

        final Exchange exchange =
                new Exchange(Exchange.Type.Topic, "test_routing_keys_exchange");

        exchange.getOptions().setAutoDelete(true);

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        publisher = publisherProvider.forExchange(exchange).onRoute("foo.biz.bar");

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange, queue)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertTrue(listener.getItem() != null);

        cleanup(queue);
        cleanup(exchange);
    }

    @Test
    public void test_multiple_queues_single_listener() throws
                                                       IOException,
                                                       InterruptedException,
                                                       NoSuchAlgorithmException,
                                                       KeyManagementException,
                                                       URISyntaxException {

        final Queue boundQueue = autoDelete(new Queue("consumer.test_multiple_queues_single_listener", Optional.of(Arrays.asList("registry.#"))));
        final Queue unboundQueue = autoDelete(new Queue("consumer.unbound_test_multiple_queues_single_listener", Optional.of(Arrays.asList("unbound.#"))));

        final Exchange exchange = new Exchange(Exchange.Type.Topic, "test_multiple_queues_single_listener");

        final RoutingPublisher<EventBase> routablePublisher = new PublisherProviderImpl<>(getTestChannelProvider()).forExchange(exchange);

        CachingDataListener listener = new CachingDataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange, boundQueue, unboundQueue)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        routablePublisher.onRoute("registry.biz.baz").publish(d);

        routablePublisher.onRoute("unbound.biz.baz").publish(d);

        Thread.sleep(500);

        assertEquals(2, listener.getItems().size());

        cleanup(boundQueue);
        cleanup(unboundQueue);
        cleanup(exchange);
    }

    @Test
    public void test_routing_keys_fails() throws
                                          IOException,
                                          InterruptedException,
                                          NoSuchAlgorithmException,
                                          KeyManagementException,
                                          URISyntaxException {

        final Queue queue = autoDelete(new Queue("consumer.testrouting_key_fails", Optional.of(Arrays.asList("foo.*.bar"))));

        final Exchange exchange = new Exchange(Exchange.Type.Topic, "test_routing_keys_exchange");

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        publisher = publisherProvider.forExchange(exchange).onRoute("foo1.biz.bar");

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange, queue)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertTrue(listener.getItem() == null);

        cleanup(queue);
        cleanup(exchange);
    }

    @Test
    public void test_routing_keys_hash_routing() throws
                                                 IOException,
                                                 InterruptedException,
                                                 NoSuchAlgorithmException,
                                                 KeyManagementException,
                                                 URISyntaxException {

        final Queue queue = autoDelete(new Queue("consumer.test_routing_keys_hash_routing", Optional.of(Arrays.asList("#.bar"))));

        final Exchange exchange = new Exchange(Exchange.Type.Topic, "test_routing_keys_exchange");

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        publisher = publisherProvider.forExchange(exchange).onRoute("foo1.biz.bar");

        DataListener listener = new DataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange, queue)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        publisher.publish(d);

        Thread.sleep(500);

        assertTrue(listener.getItem() != null);

        cleanup(queue);
        cleanup(exchange);
    }

    @Test
    public void test_multiple_routing_keys() throws
                                             IOException,
                                             InterruptedException,
                                             NoSuchAlgorithmException,
                                             KeyManagementException,
                                             URISyntaxException {

        final Queue queue = autoDelete(new Queue("consumer.test_multiple_routing_keys", Optional.of(Arrays.asList("#.bar", "#.baz.#"))));

        final Exchange exchange = new Exchange(Exchange.Type.Topic, "test_multiple_routing_keys");

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        CachingDataListener listener = new CachingDataListener(getTestChannelProvider(), autoDelete(new QueueConfiguration(exchange, queue)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        final RoutingPublisher<Data> dataRoutingPublisher = publisherProvider.forExchange(exchange);

        dataRoutingPublisher.onRoute("foo.biz.bar").publish(d);

        dataRoutingPublisher.onRoute("shimsham.boo.bing.baz.boo").publish(d);

        Thread.sleep(500);

        assertEquals(2, listener.getItems().size());

        cleanup(queue);
        cleanup(exchange);
    }

    @Test
    public void test_graceful_stop() throws Throwable {
        Exchange exchange = new Exchange(Exchange.Type.Direct, "test_graceful_stop");

        // explicitly don't auto delete yet since we're going to disconnect
        // but we want the queue to exist anyways for the second listener
        QueueConfiguration queueConfiguration = new QueueConfiguration(exchange);

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        ChannelOptions channelOptions = ChannelOptions.builder()
                                                      .prefetchCount(3)
                                                      .build();

        // create a listener
        DataListener listener = new DataListener(getTestChannelProvider(channelOptions), queueConfiguration);

        // create a second one
        DataListener dataListener2 = new DataListener(getTestChannelProvider(channelOptions), queueConfiguration);

        final int[] count = { 0 };
        final int[] listener1count = { 0 };

        Semaphore waitForMessage = new Semaphore(1);
        Semaphore stopMessage = new Semaphore(1);

        stopMessage.acquire();

        waitForMessage.acquire();

        Consumer<Data> counter = i -> {
            count[0]++;
            if (count[0] == 1) {
                waitForMessage.release();

                listener1count[0]++;

                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    logger.error(e, "Error");
                }

                stopMessage.release();
            }
            if (count[0] == 3) {
                waitForMessage.release();
            }
        };

        listener.setConsumer(counter);

        // start the first
        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        final RoutingPublisher<Data> dataRoutingPublisher = publisherProvider.forExchange(exchange);

        dataRoutingPublisher.onRoute(exchange.getExchangeName()).publish(d);
        dataRoutingPublisher.onRoute(exchange.getExchangeName()).publish(d);
        dataRoutingPublisher.onRoute(exchange.getExchangeName()).publish(d);

        // stop and it should consume at least something
        waitForMessage.acquire();

        listener.stop();

        stopMessage.acquire();

        assertThat(listener.getItem()).isNotNull();

        assertThat(count[0] > 1);

        dataListener2.setConsumer(counter);

        // start the second guy, there should be more messages

        dataListener2.start();

        waitForMessage.tryAcquire(3, TimeUnit.SECONDS);

        dataListener2.stop();

        // make sure all messages were actually processed
        assertThat(count[0]).isEqualTo(3);
        assertThat(listener1count[0]).isEqualTo(1);

        cleanup(exchange);
        cleanup(queueConfiguration);
    }

    @Test
    public void test_publish_with_options() throws
                                            IOException,
                                            InterruptedException,
                                            NoSuchAlgorithmException,
                                            KeyManagementException,
                                            URISyntaxException {

        final Queue queue = autoDelete(new Queue("consumer.test_publish_with_default_options", Optional.of(Arrays.asList("#.bar", "#.baz.#"))));

        final Exchange exchange = new Exchange(Exchange.Type.Topic, "test_publish_with_default_options");

        exchange.getOptions().setAutoDelete(true);

        final PublisherProviderImpl<Data> publisherProvider = new PublisherProviderImpl<>(getTestChannelProvider());

        ChannelOptions channelOptions = ChannelOptions.builder()
                                                      .prefetchCount(1)
                                                      .build();

        CachingDataListener listener = new CachingDataListener(getTestChannelProvider(channelOptions), autoDelete(new QueueConfiguration(exchange, queue)),
                                                               Duration.ofMillis(300));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        final RoutingPublisher<Data> dataRoutingPublisher = publisherProvider.forExchange(exchange);

        PublisherOptions options = PublisherOptions.builder()
                                                   .messageTtl(Duration.ofMillis(1))
                                                   .build();

        // publish 3 messages in th queue, listener will pause in bettween
        dataRoutingPublisher.onRoute("foo.biz.bar").publish(d, options);
        dataRoutingPublisher.onRoute("foo.biz.bar").publish(d, options);
        dataRoutingPublisher.onRoute("foo.biz.bar").publish(d, options);
        Thread.sleep(1000);

        assertEquals(1, listener.getItems().size());

        cleanup(queue);
        cleanup(exchange);
    }

}
