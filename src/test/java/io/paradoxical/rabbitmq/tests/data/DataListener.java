package io.paradoxical.rabbitmq.tests.data;

import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.QueueListenerSync;
import io.paradoxical.rabbitmq.results.MessageResult;
import com.godaddy.logging.Logger;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.function.Consumer;

import static com.godaddy.logging.LoggerFactory.getLogger;

public class DataListener extends QueueListenerSync<Data> {
    private static final Logger logger = getLogger(DataListener.class);

    @Getter
    private Data item;

    @Setter
    private Consumer<Data> consumer = i -> {};

    public DataListener(final ChannelProvider channelProvider, final QueueConfiguration info) throws
                                                                                           IOException,
                                                                                           InterruptedException,
                                                                                           NoSuchAlgorithmException,
                                                                                           KeyManagementException,
                                                                                           URISyntaxException {
        this(channelProvider, info, ListenerOptions.Default);
    }

    public DataListener(final ChannelProvider channelProvider, final QueueConfiguration info, ListenerOptions options) throws
                                                                                              IOException,
                                                                                              InterruptedException,
                                                                                              NoSuchAlgorithmException,
                                                                                              KeyManagementException,
                                                                                              URISyntaxException {
        super(channelProvider, info, Data.class, options);
    }

    @Override
    public MessageResult onMessage(final Data item) {
        this.item = item;

        consumer.accept(item);

        return MessageResult.Ack;
    }
}
