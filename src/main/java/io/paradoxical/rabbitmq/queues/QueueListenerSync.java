package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Endpoint;
import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.results.MessageResult;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Future;

public abstract class QueueListenerSync<T extends EventBase> extends QueueListenerAsync<T> implements AutoCloseable {

    private final QueueEndpoint _endpoint;

    public QueueListenerSync(
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

    public QueueListenerSync(
            ChannelProvider channelProvider,
            QueueConfiguration info,
            Class<T> target,
            ListenerOptions options) throws
                                     IOException,
                                     InterruptedException,
                                     NoSuchAlgorithmException,
                                     KeyManagementException,
                                     URISyntaxException {
        super(channelProvider, info, target, options);

        _endpoint = new QueueEndpoint(channelProvider, info.getExchange(), info.getQueues());

        _endpoint.createChannel();
    }

    @Override
    protected Endpoint getEndpoint() {
        return _endpoint;
    }

    @Override
    public Future<MessageResult> onMessageAsync(final Message<T> item) {
        return ConcurrentUtils.constantFuture(onMessage(item.getItem()));
    }

    public abstract MessageResult onMessage(final T item);

    @Override
    public void close() {
        _endpoint.close();
    }
}
