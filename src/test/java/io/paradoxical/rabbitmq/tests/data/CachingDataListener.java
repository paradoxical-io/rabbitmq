package io.paradoxical.rabbitmq.tests.data;

import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.QueueListenerSync;
import io.paradoxical.rabbitmq.results.MessageResult;
import lombok.Getter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class CachingDataListener extends QueueListenerSync<Data> {
    @Getter
    private List<Data> items = new ArrayList<>();

    Duration sleepBetweenMessages;

    public CachingDataListener(
            final ChannelProvider channelProvider,
            final QueueConfiguration info,
            Duration sleepBetweenMessages) throws
                                           IOException,
                                           InterruptedException,
                                           NoSuchAlgorithmException,
                                           KeyManagementException,
                                           URISyntaxException {
        super(channelProvider, info, Data.class);
        this.sleepBetweenMessages = sleepBetweenMessages;
    }

    public CachingDataListener(final ChannelProvider channelProvider, final QueueConfiguration info)
            throws
            IOException,
            InterruptedException,
            NoSuchAlgorithmException,
            KeyManagementException,
            URISyntaxException {
        this(channelProvider, info, Duration.ZERO);
    }


    @Override
    public MessageResult onMessage(final Data item) {

        this.items.add(item);

        try {
            Thread.sleep(this.sleepBetweenMessages.toMillis());
        }
        catch (InterruptedException ex) {

        }


        return MessageResult.Ack;
    }
}
