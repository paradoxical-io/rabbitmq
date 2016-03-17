package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.results.MessageResult;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class QueueListenerRaw<T extends EventBase> extends QueueListenerAsync<T> {
    public QueueListenerRaw(
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

    public QueueListenerRaw(
            final ChannelProvider channelProvider,
            final QueueConfiguration info,
            final Class<T> target,
            final ListenerOptions options)
            throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        super(channelProvider, info, target, options);
    }

    @Override
    public Future<MessageResult> onMessageAsync(final Message<T> item) {
        return CompletableFuture.completedFuture(onMessage(item));
    }

    public abstract MessageResult onMessage(final Message<T> item);
}
