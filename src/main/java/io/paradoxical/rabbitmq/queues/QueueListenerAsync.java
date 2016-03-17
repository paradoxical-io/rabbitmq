package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Endpoint;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.FutureEventProcessor;
import io.paradoxical.rabbitmq.ListenerBase;
import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.Queue;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.RabbitRawMessage;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public abstract class QueueListenerAsync<T extends EventBase> extends ListenerBase<T> implements AutoCloseable, OnMessageInvocation<T> {

    private final QueueEndpoint _endpoint;

    public QueueListenerAsync(
            ChannelProvider channelProvider,
            QueueConfiguration info,
            Class<T> target) throws
                             IOException,
                             InterruptedException,
                             NoSuchAlgorithmException,
                             KeyManagementException,
                             URISyntaxException {
        this(channelProvider, info, target, ListenerOptions.Default);
    }

    public QueueListenerAsync(
            ChannelProvider channelProvider,
            QueueConfiguration info,
            Class<T> target,
            ListenerOptions options) throws
                                     IOException,
                                     InterruptedException,
                                     NoSuchAlgorithmException,
                                     KeyManagementException,
                                     URISyntaxException {
        super(target, channelProvider, options);

        _endpoint = new QueueEndpoint(channelProvider, info.getExchange(), info.getQueues());

        _endpoint.createChannel();
    }

    @Override protected Consumer getRmqConsumerType(
            final Channel channel, final Exchange exchange, final Queue queue) {
        FutureEventProcessor<T> onMessageAsync = this::onMessageAsync;

        return new DefaultConsumer(channel) {
            @Override public void handleDelivery(
                    final String consumerTag,
                    final Envelope envelope,
                    final AMQP.BasicProperties properties,
                    final byte[] body) throws
                                       IOException {
                RabbitRawMessage rabbitRawMessage = new RabbitRawMessage(consumerTag, envelope, properties, body, exchange);

                deliveryHandler(channel, queue, rabbitRawMessage, onMessageAsync);
            }
        };
    }

    @Override
    protected Endpoint getEndpoint() {
        return _endpoint;
    }

    @Override
    public void close() throws Exception {
        _endpoint.close();
    }
}


