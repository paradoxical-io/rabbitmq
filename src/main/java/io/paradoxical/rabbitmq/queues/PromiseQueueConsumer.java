package io.paradoxical.rabbitmq.queues;

import com.godaddy.logging.Logger;
import com.rabbitmq.client.QueueingConsumer;
import io.paradoxical.rabbitmq.ListenerOptions;
import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.RabbitRawMessage;
import io.paradoxical.rabbitmq.SingleQueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.results.MessageResult;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.godaddy.logging.LoggerFactory.getLogger;


public class PromiseQueueConsumer<T extends EventBase> extends BlockingQueueConsumerSyncBase<T> {

    private static final Logger logger = getLogger(PromiseQueueConsumer.class);

    public PromiseQueueConsumer(final ChannelProvider channelProvider, final SingleQueueConfiguration info, final Class<T> target)
            throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        super(channelProvider, info, target);
    }

    public PromiseQueueConsumer(final ChannelProvider channelProvider, final SingleQueueConfiguration info, final Class<T> target, final ListenerOptions options)
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
    public synchronized MessagePromise<T> getNextMessage(Duration timeout) {
        try {
            if (consumer != null) {

                QueueingConsumer.Delivery delivery = consumer.getConsumer().nextDelivery(timeout.toMillis());

                // timeout
                if (delivery == null) {
                    return EmptyMessagePromise.instance();
                }

                RabbitRawMessage rabbitRawMessage = new RabbitRawMessage(consumer.getConsumer().getConsumerTag(),
                                                                         delivery.getEnvelope(),
                                                                         delivery.getProperties(),
                                                                         delivery.getBody(),
                                                                         consumer.getExchange());

                final CompletableFuture<MessageResult> completableFuture = new CompletableFuture<>();
                final CompletableFuture<Message<T>> messageFuture = new CompletableFuture<>();

                final MessagePromise<T> messagePromise = new MessagePromise<T>() {

                    @Override
                    public void complete(final MessageResult result) {
                        completableFuture.complete(result);
                    }

                    @Override
                    public Optional<Message<T>> getMessage() throws ExecutionException, InterruptedException {
                        return Optional.of(messageFuture.get());
                    }
                };

                Executors.newCachedThreadPool().submit(
                        () -> {
                            try {
                                deliveryHandler(consumer.getConsumer().getChannel(),
                                                consumer.getQueue(),
                                                rabbitRawMessage,
                                                msg -> {
                                                    messageFuture.complete(msg);
                                                    return completableFuture;
                                                });
                            }
                            catch (Throwable e) {
                                logger.error(e, "Error processing message asynchronously");
                                messageFuture.completeExceptionally(e);
                            }
                        });

                return messagePromise;
            }
        }
        catch (Throwable ex) {
            logger.error(ex, "Error getting message!");
        }

        return EmptyMessagePromise.instance();
    }
}
