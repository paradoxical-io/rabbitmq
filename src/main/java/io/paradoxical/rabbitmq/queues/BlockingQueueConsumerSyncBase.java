package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Endpoint;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.ListenerBase;
import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.Queue;
import io.paradoxical.rabbitmq.SingleQueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import com.godaddy.logging.Logger;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import lombok.Value;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

import static com.godaddy.logging.LoggerFactory.getLogger;

public abstract class BlockingQueueConsumerSyncBase<T extends EventBase> extends ListenerBase<T> implements AutoCloseable {
    @Value
    class BlockingRmqConsumer {
        private BlockingConsumer consumer;

        private Exchange exchange;

        private Queue queue;
    }

    protected final QueueEndpoint _endpoint;

    protected BlockingRmqConsumer consumer;

    private static final Logger logger = getLogger(CallbackBasedQueueConsumer.class);

    public BlockingQueueConsumerSyncBase(
            ChannelProvider channelProvider,
            SingleQueueConfiguration info,
            Class<T> target) throws
                             IOException,
                             InterruptedException,
                             NoSuchAlgorithmException,
                             KeyManagementException,
                             URISyntaxException {
        this(channelProvider, info, target, ListenerOptions.Default);
    }

    public BlockingQueueConsumerSyncBase(
            ChannelProvider channelProvider,
            SingleQueueConfiguration info,
            Class<T> target,
            ListenerOptions options) throws
                                     IOException,
                                     InterruptedException,
                                     NoSuchAlgorithmException,
                                     KeyManagementException,
                                     URISyntaxException {
        super(target, channelProvider, options);

        _endpoint = new QueueEndpoint(channelProvider, info.getExchange(), Collections.singletonList(info.getQueue()));

        _endpoint.createChannel();
    }

    @Override
    protected Consumer getRmqConsumerType(final Channel channel, final Exchange exchange, final Queue queue) {
        consumer = new BlockingRmqConsumer(new BlockingConsumer(channel), exchange, queue);

        return consumer.getConsumer();
    }

    @Override
    protected Endpoint getEndpoint() {
        return _endpoint;
    }

    @Override
    public void close() throws Exception {
        _endpoint.close();

        consumer = null;
    }
}
