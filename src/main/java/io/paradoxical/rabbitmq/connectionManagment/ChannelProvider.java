package io.paradoxical.rabbitmq.connectionManagment;

import com.rabbitmq.client.Channel;

public interface ChannelProvider extends AutoCloseable {
    Channel getChannel();
}
