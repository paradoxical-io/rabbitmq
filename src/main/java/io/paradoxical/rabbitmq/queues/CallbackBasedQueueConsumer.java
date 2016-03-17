package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.EventProcessor;
import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.RabbitRawMessage;
import io.paradoxical.rabbitmq.SingleQueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.results.MessageResult;
import com.godaddy.logging.Logger;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.godaddy.logging.LoggerFactory.getLogger;

public class CallbackBasedQueueConsumer<T extends EventBase> extends BlockingQueueConsumerSyncBase<T> implements AutoCloseable {

    private static final Logger logger = getLogger(CallbackBasedQueueConsumer.class);

    public CallbackBasedQueueConsumer(
            final ChannelProvider channelProvider,
            final SingleQueueConfiguration info, final Class<T> target)
            throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        super(channelProvider, info, target);
    }

    public CallbackBasedQueueConsumer(final ChannelProvider channelProvider, final SingleQueueConfiguration info, final Class<T> target, final ListenerOptions options)
            throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        super(channelProvider, info, target, options);
    }

    /**
     * Manually invoke pulling an event off the wire and block up to the duration
     *
     * Returns true if a message was consumed
     *
     * @param timeout
     */
    public synchronized boolean pullNextBlockingRmqMessage(Duration timeout, EventProcessor<T> processor) {
        try {
            if (consumer != null) {
                QueueingConsumer.Delivery delivery = consumer.getConsumer().nextDelivery(timeout.toMillis());

                // timeout
                if (delivery == null) {
                    return false;
                }

                RabbitRawMessage rabbitRawMessage = new RabbitRawMessage(consumer.getConsumer().getConsumerTag(),
                                                                         delivery.getEnvelope(),
                                                                         delivery.getProperties(),
                                                                         delivery.getBody(),
                                                                         consumer.getExchange());

                deliveryHandler(consumer.getConsumer().getChannel(),
                                consumer.getQueue(),
                                rabbitRawMessage,
                                msg -> {
                                    MessageResult messageResult = processor.handleMessage(msg);

                                    return CompletableFuture.completedFuture(messageResult);
                                });

                return true;
            }
        }
        catch (Throwable ex) {
            logger.error(ex, "Error getting message!");
        }

        return false;
    }
}
