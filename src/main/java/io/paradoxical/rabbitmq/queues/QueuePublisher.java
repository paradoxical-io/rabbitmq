package io.paradoxical.rabbitmq.queues;

import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import com.rabbitmq.client.AMQP;
import io.paradoxical.rabbitmq.DefaultSerializer;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.HeaderConstants;
import io.paradoxical.rabbitmq.Publisher;
import io.paradoxical.rabbitmq.PublisherExchange;
import io.paradoxical.rabbitmq.PublisherOptions;
import io.paradoxical.rabbitmq.PublishingContext;
import io.paradoxical.rabbitmq.QueueSerializer;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.connectionManagment.ExchangeUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class QueuePublisher<T extends EventBase> extends QueueEndpoint implements Publisher<T> {

    private static final Logger logger = LoggerFactory.getLogger(QueuePublisher.class);

    private static ConcurrentHashMap<String, Boolean> cachedDeclaredExchangeNames = new ConcurrentHashMap<>();

    private final PublisherExchange publisherConfig;

    private final QueueSerializer serializer;

    private final Supplier<UUID> corrIdProvider;

    public QueuePublisher(
            final ChannelProvider channelProvider,
            final PublisherExchange exchange,
            final Supplier<UUID> corrIdProvider) throws KeyManagementException,
                                                        NoSuchAlgorithmException,
                                                        IOException,
                                                        URISyntaxException {
        this(channelProvider, exchange, corrIdProvider, new DefaultSerializer());
    }

    public QueuePublisher(
            final ChannelProvider channelProvider,
            final PublisherExchange exchange,
            final Supplier<UUID> corrIdProvider,
            final QueueSerializer serializer) throws
                                              URISyntaxException,
                                              KeyManagementException,
                                              NoSuchAlgorithmException,
                                              IOException {
        super(channelProvider, exchange.getExchange(), null);

        this.corrIdProvider = corrIdProvider;
        this.publisherConfig = exchange;
        this.serializer = serializer;
    }

    @Override
    public void publish(T item, PublisherOptions options) throws IOException {
        publishInternal(item, options);
    }

    @Override
    public void publish(T item) throws IOException {
        publishInternal(item, PublisherOptions.Default);
    }

    private void publishInternal(T item, PublisherOptions options) throws IOException {

        if (getChannel() == null) {
            createChannel();
        }

        declareExchange(exchange);

        Logger publishLoggingCtx = LoggerFactory.getLogger(QueuePublisher.class);

        publishLoggingCtx.with("exchange", getExchange().getExchangeName())
                         .with("route", publisherConfig.getRoute());

        if (item == null) {
            publishLoggingCtx.warn("Attempting top publish null to queue!");

            return;
        }

        String dataType = item.getClass().getName();

        publishLoggingCtx = publishLoggingCtx.with("dataType", dataType);

        if (corrIdProvider != null && item.getCorrelationId() == null) {
            item.setCorrelationId(corrIdProvider.get());
        }
        else if (item.getCorrelationId() == null) {
            logger.warn("Message does not have a correlation id (auto generating one), and corrIdProvider was null.");

            item.setCorrelationId(UUID.randomUUID());
        }

        publishLoggingCtx.with(item)
                         .with("publish-count", Optional.ofNullable(options.getContext())
                                                        .map(PublishingContext::getPreviousPublishes)
                                                        .orElse(0))
                         .info("Publishing item");

        final AMQP.BasicProperties properties = getProperties(item, options);

        Exchange endpoint = getExchange();

        String routingKey = publisherConfig.getRoute();

        getChannel().basicPublish(
                endpoint.getExchangeName(),
                routingKey,
                properties,
                serialize(item));

        close();
    }

    /**
     * Prevent excess over the wire calls to RMQ by caching if we've already declared an exchange
     */
    private void declareExchange(final Exchange exchange) throws IOException {
        String exchangeName = exchange.getExchangeName();

        if (!cachedDeclaredExchangeNames.containsKey(exchangeName)) {
            try {
                ExchangeUtils.declare(getChannel(), exchange);
            }
            catch (IOException ex) {
                // failed to create exchange due maybe to configuration issues, try and ignore
                // this causes the underlying channel to close, so we should make sure to reopen it
                // if thats the case the exchange already exists, so it doesn't matter if we failed to declare it

                logger.warn(ex, "Error re-declaring exchange");

                createChannel();
            }

            cachedDeclaredExchangeNames.put(exchangeName, true);
        }
    }

    protected AMQP.BasicProperties getProperties(T item, PublisherOptions options) {
        HashMap<String, Object> headers = new HashMap<>();

        int publishingAttempts = Optional.ofNullable(options.getContext())
                                         .flatMap(ctx -> Optional.ofNullable(ctx.getPreviousPublishes()))
                                         .orElse(0) + 1;

        headers.put(HeaderConstants.PublishAttempts, publishingAttempts);

        return new AMQP.BasicProperties
                .Builder()
                .correlationId(item.getCorrelationId().toString())
                .headers(headers)
                .expiration(options.getMessageTtl() != null ? Long.toString(options.getMessageTtl().toMillis()) : null)
                .build();
    }

    protected byte[] serialize(T item) throws IOException {
        try {
            return serializer.writeBytes(item);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}