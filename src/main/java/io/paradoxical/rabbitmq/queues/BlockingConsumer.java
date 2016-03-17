package io.paradoxical.rabbitmq.queues;

import com.godaddy.logging.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.godaddy.logging.LoggerFactory.getLogger;

public class BlockingConsumer extends DefaultConsumer {
    private static final Logger logger = getLogger(BlockingConsumer.class);

    private final BlockingQueue<QueueingConsumer.Delivery> blockingQueue;

    public BlockingConsumer(final Channel channel) {
        this(channel, new LinkedBlockingQueue<>());
    }

    public BlockingConsumer(final Channel channel, final BlockingQueue<QueueingConsumer.Delivery> blockingQueue) {
        super(channel);

        this.blockingQueue = blockingQueue;
    }

    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
        try {
            blockingQueue.put(new QueueingConsumer.Delivery(envelope, properties, body));
        }
        catch (InterruptedException e) {
            logger.error(e, "Error putting message into blocking queue!");

            throw new RuntimeException(e);
        }
    }

    public QueueingConsumer.Delivery nextDelivery(final long timeoutMillis) throws InterruptedException {
        return blockingQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }
}
