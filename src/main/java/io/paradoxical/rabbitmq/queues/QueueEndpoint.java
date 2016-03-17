package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Endpoint;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.Queue;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class QueueEndpoint implements AutoCloseable, Endpoint {

    private static final Logger logger = getLogger(QueueEndpoint.class);
    private final ChannelProvider channelProvider;
    protected final Exchange exchange;
    private final List<Queue> queues;
    private Channel channel;

    public QueueEndpoint(ChannelProvider channelProvider, Exchange exchange, List<Queue> queues) throws
                                                                                          IOException,
                                                                                          NoSuchAlgorithmException,
                                                                                          KeyManagementException,
                                                                                          URISyntaxException {
        this.channelProvider = channelProvider;
        this.exchange = exchange;
        this.queues = queues;
    }

    protected Channel createChannel() throws IOException {
        close();

        channel = channelProvider.getChannel();

        return channel;
    }

    public Channel getChannel() {
        return channel;
    }

    public Exchange getExchange() {
        return exchange;
    }

    public void close() {
        try {
            if(channel != null && channel.isOpen()) {
                channel.close();
                channel = null;
            }
        }
        catch (Exception ex) {
            logger.error("Error closing endpoint", ex);
        }
    }

    public List<Queue> getQueues() {
        return queues;
    }
}
