package io.paradoxical.rabbitmq;

import com.rabbitmq.client.Channel;

import java.util.List;

public interface Endpoint {
    Channel getChannel();

    Exchange getExchange();

    List<Queue> getQueues();
}
