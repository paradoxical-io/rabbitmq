package io.paradoxical.rabbitmq.tests.data;

import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.QueueListenerSync;
import io.paradoxical.rabbitmq.results.MessageResult;
import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.Semaphore;

public class DataListenerRetriable extends QueueListenerSync<Data> {

    private static final Logger logger = LoggerFactory.getLogger(DataListenerRetriable.class);

    @Getter
    private Data item;

    @Getter
    private int count = 0;

    @Getter @Setter
    private Semaphore semaphore;

    private UUID instance = UUID.randomUUID();

    public DataListenerRetriable(final ChannelProvider channelProvider, final QueueConfiguration info) throws
                                                                                                       IOException,
                                                                                              InterruptedException,
                                                                                                       NoSuchAlgorithmException,
                                                                                                       KeyManagementException,
                                                                                                       URISyntaxException {
        this(channelProvider, info, ListenerOptions.Default);
    }

    public DataListenerRetriable(final ChannelProvider channelProvider, final QueueConfiguration info, ListenerOptions options) throws
                                                                                                                       IOException,
                                                                                                                       InterruptedException,
                                                                                                                       NoSuchAlgorithmException,
                                                                                                                       KeyManagementException,
                                                                                                                       URISyntaxException {
        super(channelProvider, info, Data.class, options);
    }

    @Override
    public MessageResult onMessage(final Data item) {
        logger.info("instance got message: " + instance);

        this.item = item;

        count++;

        return MessageResult.RetryLater;
    }
}
