package io.paradoxical.rabbitmq;



import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import io.paradoxical.rabbitmq.metrics.MetricsEventManager;
import io.paradoxical.rabbitmq.metrics.MetricsEventManagerOptions;
import io.paradoxical.rabbitmq.metrics.delegators.DelegatedCounter;
import io.paradoxical.rabbitmq.metrics.delegators.DelegatedTimer;
import io.paradoxical.rabbitmq.queues.EventBase;
import org.jboss.logging.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ListenerBase<T extends EventBase> {

    private static final Logger logger = LoggerFactory.getLogger(ListenerBase.class);

    private final Class<T> target;

    private final ChannelProvider channelProvider;
    private final ListenerOptions options;

    private final List<String> consumingQueueTags = new ArrayList<>();

    private final AtomicInteger messagesInProgress;

    private final Semaphore drainingSemaphore = new Semaphore(1);

    private final MetricsEventManager<T> metricsManager;

    private volatile boolean running = false;

    public ListenerBase(Class<T> targetType, ChannelProvider channelProvider, ListenerOptions options) {
        this.target = targetType;
        this.channelProvider = channelProvider;
        this.options = options;

        messagesInProgress = new AtomicInteger(0);

        metricsManager = new MetricsEventManager<>(MetricsEventManagerOptions.<T>builder()
                                                           .classMapper(this::getEventClassType)
                                                           .metricGroups(options.getMetricGroups())
                                                           .sourceClass(this.getClass())
                                                           .metricRegistry(options.getMetricRegistry())
                                                           .build());
    }


    public void start() {
        try {
            running = true;

            start(getEndpoint().getChannel());
        }
        catch (InterruptedException | IOException e) {
            logger.error("Error running listener", e);
        }
    }

    protected synchronized void start(final Channel channel) throws InterruptedException, IOException {

        Endpoint endpoint = getEndpoint();

        for (Queue originalQueue : endpoint.getQueues()) {

            Queue initializedQueue = ChannelInitializer.initializeExchange(channel, endpoint.getExchange(), originalQueue);

            String tag = channel.basicConsume(initializedQueue.getName(), false, getRmqConsumerType(channel, endpoint.getExchange(), initializedQueue));

            logger.info("Running listener with tag=" + tag);

            consumingQueueTags.add(tag);
        }
    }

    /**
     * Defaults to a threadpool based push consumer
     *
     * @param channel
     * @param exchange
     * @param queue
     * @return
     */
    protected abstract Consumer getRmqConsumerType(final Channel channel, final Exchange exchange, final Queue queue);

    /**
     * Process a message off the wire. Set up MDC context, deal with stopping sematnics, etc.
     * delegate actual work of processing message elsewhere though
     *
     * @param channel
     * @param queue
     * @param message
     * @throws IOException
     */
    protected void deliveryHandler(
            final Channel channel,
            final Queue queue,
            final RabbitRawMessage message,
            final FutureEventProcessor<T> callback) throws IOException {

        DelegatedTimer.Context firehoseTimer = null;

        try {
            if (!running) {
                logger.warn("Message received on dead channel, attempting nack but not expecting success");

                nack(message.getEnvelope(), channel, true, null);

                return;
            }
        }
        catch (Throwable nackException) {
            if (!running) {
                logger.warn("Stopped listener cannot respond on channel", nackException);
            }
            else {
                logger.error("Unhandled exception during dequeuing. Attempting nack!", nackException);
            }
        }

        try {
            messagesInProgress.incrementAndGet();

            logger.debug("Got message on listener tag=" + message.getConsumerTag() + " message-delivery-tag=" + message.getEnvelope().getDeliveryTag());

            firehoseTimer = getFirehoseTimer().time();

            getInFlightFirehoseCounter().inc();

            process(message, channel, queue, callback);
        }
        catch (Throwable processingException) {
            if (!running) {
                logger.warn("Stopped listener cannot respond on channel", processingException);
            }
            else {
                logger.error("Unhandled exception during dequeuing. Attempting nack!", processingException);
            }

            try {
                nack(message.getEnvelope(), channel, !message.getEnvelope().isRedeliver(), null);

                logger.warn("Unhandled exception was able to successfully nack message but message may not have be properly processed.");
            }
            catch (Throwable nackException) {
                if (!running) {
                    logger.warn("Channel closed, unable to re-nack");

                    return;
                }

                logger.error("Nack unable to proceed! " +
                             "Message may be stuck in dead state and the consumer connection should be restarted. tag=" +
                             message.getConsumerTag(), nackException);
            }
        }
        finally {
            messagesInProgress.decrementAndGet();
            drainingSemaphore.release();

            if (running) {

                if (firehoseTimer != null) {
                    firehoseTimer.stop();

                    getInFlightFirehoseCounter().dec();
                }

                logger.debug("Completing message - message-delivery-tag=" + message.getEnvelope().getDeliveryTag());
            }
        }
    }


    /**
     * Allow subclasses to give a more specific type of what the current event type is.
     * This allows for wrapped event types to show up as the inner type
     *
     * @param item
     * @return
     */
    protected Class<?> getEventClassType(T item) {
        return item.getClass();
    }

    /**
     * Main processing of a message.
     *
     * @param rabbitRawMessage
     * @param channel
     * @param queue
     * @param callback
     * @throws IOException
     */
    private void process(final RabbitRawMessage rabbitRawMessage, final Channel channel, final Queue queue, final FutureEventProcessor<T> callback) throws IOException {
        Message<T> message = null;

        try {
            message = getMessageFromRaw(rabbitRawMessage, queue);

            T item = message.getItem();

            DelegatedTimer.Context eventTypeTimer = getEventTypeTimer(item).time();

            getEventTypeInFlightCounter(item).inc();

            try {
                delegateMessage(message, channel, callback);
            }
            finally {
                eventTypeTimer.stop();

                getEventTypeInFlightCounter(item).dec();
            }
        }
        catch (Throwable ex) {
            logger.error(String.format("Error during dequeueing. Tag %s", rabbitRawMessage.getEnvelope().getDeliveryTag()),
                         ex);

            // we got a message object, leverage the requeue semantics by retry if possible
            if (message != null) {
                requeue(message, channel);
            }
            else {
                // redeliver if it wasn't already delivered once, dont know anything else about the message
                nack(rabbitRawMessage.getEnvelope(), channel, !rabbitRawMessage.getEnvelope().isRedeliver(), null);
            }
        }
        finally {
            cleanMessageContext();
        }
    }

    protected Message<T> getMessageFromRaw(final RabbitRawMessage rabbitRawMessage, Queue queue) throws IOException {
        Message<T> message = new Message<>();

        message.setRabbitRawMessage(rabbitRawMessage);

        message.setAtLeastOnceDelivery(rabbitRawMessage.getEnvelope().isRedeliver());

        UUID correlationId = ChannelInitializer.setupMessageContext(rabbitRawMessage, getEndpoint().getExchange(), queue);

        message.setItem(getDeserializedItem(rabbitRawMessage, correlationId));

        return message;
    }

    private DelegatedTimer getFirehoseTimer() {
        return metricsManager.getTimer("events");
    }

    private DelegatedTimer getEventTypeTimer(T item) {
        return metricsManager.getTimerByEventType(item, "events");
    }

    private DelegatedCounter getEventTypeRetryCounter(T item) {
        return metricsManager.getCounterByEventType(item, "retries");
    }

    private DelegatedCounter getRetryFirehoseCounter() {
        return metricsManager.getCounter("retries");
    }

    private DelegatedCounter getEventTypeInFlightCounter(T item) {
        if (item == null) {
            return new DelegatedCounter(Collections.emptyList());
        }

        return metricsManager.getCounterByEventType(item, "inflight");
    }

    private DelegatedCounter getInFlightFirehoseCounter() {
        return metricsManager.getCounter("inflight");
    }

    private DelegatedCounter getEventTypeRejectCounter(T item) {
        return metricsManager.getCounterByEventType(item, "reject");
    }

    private DelegatedCounter getRejectFirehoseCounter() {
        return metricsManager.getCounter("reject");
    }


    /**
     * Clean up MDC context
     */
    private void cleanMessageContext() {
        MDC.remove(FilterAttributes.CORR_ID);
    }

    /**
     * Deserialize the message into the target type
     *
     * @param rabbitRawMessage
     * @param correlationId
     * @return
     * @throws IOException
     */
    private T getDeserializedItem(final RabbitRawMessage rabbitRawMessage, final UUID correlationId) throws IOException {
        T item = deserialize(rabbitRawMessage.getBody());

        if (item.getCorrelationId() == null) {
            item.setCorrelationId(correlationId);
        }

        return item;
    }


    /**
     * Delegates to subclass and processes response
     *
     * @param message
     * @param channel
     * @param callback
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void delegateMessage(final Message<T> message, final Channel channel, final FutureEventProcessor<T> callback)
            throws IOException, ExecutionException, InterruptedException {
        switch (callback.handleMessage(message).get()) {
            case Ack:
                ack(message, channel);
                break;
            case Nack:
                nack(message, channel);
                break;
            case RequeueUntilMaxTries:
                requeue(message, channel);
                break;
            case RetryLater:
                retryLater(message, channel);
                break;
            case Defer:
                defer(message, channel);
        }
    }

    /**
     * Nacks but always requeues regardless of previous requeue attempts
     *
     * @param message
     * @param channel
     */
    private void defer(final Message<T> message, final Channel channel) throws IOException {
        nack(message.getRabbitRawMessage().getEnvelope(), channel, true, message.getItem());
    }

    /**
     * Nacks or republishes with redelivery up to the configured max options
     *
     * @param message
     * @param channel
     * @throws IOException
     */
    private void requeue(final Message<T> message, final Channel channel) throws IOException {
        // if we're only allowed to retry once, we can directly use the requeue
        // if it wasn't already delivered once

        if (options.getMaxRetries() == 1) {
            nack(message.getRabbitRawMessage().getEnvelope(), channel, !message.getRabbitRawMessage().getEnvelope().isRedeliver(), message.getItem());

            return;
        }

        // otherwise, republish the event and update its header context
        // to track how many times its been published

        int publishAttempts = getPublishAttempts(message);

        if (publishAttempts < options.getMaxRetries()) {
            PublishingContext publishingContext = new PublishingContext();

            publishingContext.setPreviousPublishes(publishAttempts);

            PublisherOptions publisherOptions = PublisherOptions.builder()
                                                                .context(publishingContext)
                                                                .build();

            publishRetryMessage(message, publisherOptions, message.getRabbitRawMessage().getExchange(), channel);
        }
    }

    private void ack(final Message<T> message, final Channel channel) throws IOException {
        channel.basicAck(message.getRabbitRawMessage().getEnvelope().getDeliveryTag(), false);
    }

    private void nack(final Message<T> message, final Channel channel) throws IOException {
        nack(message.getRabbitRawMessage().getEnvelope(), channel, false, message.getItem());
    }

    private void retryLater(final Message<T> message, final Channel channel) throws IOException {

        Optional<RetryExchange> retryExchangeOption = message.getRabbitRawMessage().getExchange().getRetryExchange();

        if (!retryExchangeOption.isPresent()) {
            throw new RuntimeException("Unable to retry message, no retry exchange is declared!");
        }

        RetryExchange retryExchange = retryExchangeOption.get();

        int publishAttempts = getPublishAttempts(message);

        Optional<Duration> duration = getRetryDuration(message, publishAttempts);

        if (!duration.isPresent()) {
            logger.warn("Retry duration is none but asked to retry. Must nack");

            nack(message, channel);

            return;
        }

        if (publishAttempts >= options.getMaxRetries()) {
            logger.warn("Max publish attempts reached. publishAttempts={}, maxRetries={}", publishAttempts, options.getMaxRetries());

            nack(message, channel);

            return;
        }

        PublishingContext publishingContext = new PublishingContext();

        publishingContext.setPreviousPublishes(publishAttempts);

        PublisherOptions publisherOptions = PublisherOptions.builder()
                                                            .messageTtl(duration.get())
                                                            .context(publishingContext)
                                                            .build();

        publishRetryMessage(message, publisherOptions, retryExchange, channel);
    }

    private void publishRetryMessage(
            final Message<T> message,
            final PublisherOptions publisherOptions,
            final Exchange exchange,
            final Channel channel) throws IOException {

        PublisherProviderImpl<T> publisher = new PublisherProviderImpl<>(channelProvider, message.getItem()::getCorrelationId, options.getSerializer());

        // make sure to publish the message FIRST
        // otherwise we could have accidentally ack'd but not re-queued

        publisher.forExchange(exchange)
                 .onRoute(message.getRabbitRawMessage().getEnvelope().getRoutingKey())
                 .publish(message.getItem(), publisherOptions);

        // ack the old message
        ack(message, channel);

        updateRedeliveryCounter(message.getItem());
    }

    private int getPublishAttempts(final Message<T> message) {
        Object publishAttemptsRaw = message.getRabbitRawMessage().getProperties().getHeaders().get(HeaderConstants.PublishAttempts);

        return publishAttemptsRaw == null ? 0 : Integer.parseInt(publishAttemptsRaw.toString());
    }

    private Optional<Duration> getRetryDuration(final Message<T> message, int publishAttempts) {
        return message.getRabbitRawMessage()
                      .getExchange()
                      .getRetryExchange()
                      .flatMap(i -> i.getStrategy().nextRetry(publishAttempts, message.getItem()));
    }

    public void stop() throws IOException, TimeoutException {

        if (!running) {
            return;
        }

        running = false;

        try {
            while (messagesInProgress.get() > 0) {
                drainingSemaphore.acquire();
            }
        }
        catch (InterruptedException e) {
            logger.error("Error", e);
        }

        if (getEndpoint() != null && getEndpoint().getChannel() != null) {
            Channel channel = getEndpoint().getChannel();

            consumingQueueTags.forEach(tag -> {
                try {
                    logger.info("Stopping RMQ tag=" + tag);

                    channel.basicCancel(tag);
                }
                catch (IOException e) {
                    logger.error("Error disconnecting from channel with tag=" + tag, e);
                }
            });

            consumingQueueTags.clear();

            channel.close();
        }
    }

    protected abstract Endpoint getEndpoint();

    protected T deserialize(byte[] body) throws IOException {
        try {
            return options.getSerializer().read(body, target);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void nack(final Envelope envelope, final Channel channel, final Boolean redeliver, T item) throws IOException {
        logger.info(String.format("Nacking message tag=%s, attempting redelivery: %b",
                                  envelope.getDeliveryTag(),
                                  redeliver));

        if (redeliver) {
            updateRedeliveryCounter(item);
        }
        else {
            updateRejectCounter(item);
        }

        channel.basicNack(envelope.getDeliveryTag(),
                          false,
                          redeliver);
    }

    private void updateRejectCounter(final T item) {
        getRejectFirehoseCounter().inc();

        if (item != null) {
            getEventTypeRejectCounter(item).inc();
        }
    }

    private void updateRedeliveryCounter(final T item) {
        getRetryFirehoseCounter().inc();

        if (item != null) {
            getEventTypeRetryCounter(item).inc();
        }
    }


}