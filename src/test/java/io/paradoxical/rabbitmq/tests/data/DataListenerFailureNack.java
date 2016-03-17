package io.paradoxical.rabbitmq.tests.data;

import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.QueueListenerSync;
import io.paradoxical.rabbitmq.results.MessageResult;
import lombok.Getter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class DataListenerFailureNack extends QueueListenerSync<Data> {
    @Getter
    private int count = 0;

    public DataListenerFailureNack(
            final ChannelProvider channelProvider,
            final QueueConfiguration info) throws
                                           IOException,
                                           InterruptedException,
                                           NoSuchAlgorithmException,
                                           KeyManagementException,
                                           URISyntaxException {
        this(channelProvider, info, ListenerOptions.Default);
    }

    public DataListenerFailureNack(
            final ChannelProvider channelProvider,
            final QueueConfiguration info,
            final ListenerOptions options) throws
                                           IOException,
                                           InterruptedException,
                                           NoSuchAlgorithmException,
                                           KeyManagementException,
                                           URISyntaxException {
        super(channelProvider, info, Data.class, options);
    }


    @Override
    public MessageResult onMessage(final Data item) {
        count++;

        return MessageResult.Nack;
    }
}
