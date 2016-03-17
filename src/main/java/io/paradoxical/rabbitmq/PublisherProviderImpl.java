package io.paradoxical.rabbitmq;

import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.EventBase;
import io.paradoxical.rabbitmq.queues.QueuePublisher;
import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;

import javax.inject.Inject;
import java.util.UUID;
import java.util.function.Supplier;

public class PublisherProviderImpl<T extends EventBase> implements PublisherProvider<T> {
    private static final Logger logger = LoggerFactory.getLogger(PublisherProviderImpl.class);

    protected final ChannelProvider channelProvider;

    private final QueueSerializer serializer;
    private final Supplier<UUID> corrIdProvider;

    public PublisherProviderImpl(final ChannelProvider channelProvider) {
        this(channelProvider, () -> {
            logger.warn("Message does not have a correlation id, auto generating one");
            return UUID.randomUUID();
        });
    }

    public PublisherProviderImpl(final ChannelProvider channelProvider, final Supplier<UUID> corrIdProvider) {
        this(channelProvider, corrIdProvider, new DefaultSerializer());
    }

    @Inject
    public PublisherProviderImpl(final ChannelProvider channelProvider, final Supplier<UUID> corrIdProvider, QueueSerializer serializer) {
        this.channelProvider = channelProvider;
        this.corrIdProvider = corrIdProvider;
        this.serializer = serializer;
    }

    @Override
    public QueuePublisher<T> forExchange(Exchange exchange, String route) {
        try {
            return new QueuePublisher<>(channelProvider, new PublisherExchange(exchange, route), corrIdProvider, serializer);
        }
        catch (Exception ex) {
            logger.error(ex, "Error creating queue publisher");
        }

        return null;
    }
}