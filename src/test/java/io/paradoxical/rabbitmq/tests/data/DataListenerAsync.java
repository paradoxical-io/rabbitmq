package io.paradoxical.rabbitmq.tests.data;

import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.queues.QueueListenerAsync;
import io.paradoxical.rabbitmq.results.MessageResult;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataListenerAsync extends QueueListenerAsync<Data> {
    private static final Logger logger = LoggerFactory.getLogger(DataListenerAsync.class);
    @Getter
    private Data item;

    public DataListenerAsync(final ChannelProvider channelProvider, final QueueConfiguration info) throws
                                                                                                IOException,
                                                                                                InterruptedException,
                                                                                                NoSuchAlgorithmException,
                                                                                                KeyManagementException,
                                                                                                URISyntaxException {
        super(channelProvider, info, Data.class);
    }


    @Override public Future<MessageResult> onMessageAsync(final Message<Data> item) {
        return Executors.newCachedThreadPool().submit(() -> {

            this.item = item.getItem();

            logger.debug("executing message");

            return MessageResult.Ack;
        });
    }
}
