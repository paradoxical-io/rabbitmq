package io.paradoxical.rabbitmq;

import io.paradoxical.rabbitmq.connectionManagment.ExchangeUtils;
import com.godaddy.logging.Logger;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.MDC;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.godaddy.logging.LoggerFactory.getLogger;

public class ChannelInitializer {

    private static final Logger logger = getLogger(ChannelInitializer.class);

    /**
     * Declares the exchange and queue with RMQ, sets up DLQ semantics etc
     *
     * @param channel
     * @param exchange
     * @param queue
     * @return
     * @throws IOException
     */
    public static Queue initializeExchange(final Channel channel, final Exchange exchange, final Queue queue) throws IOException {

        // if we have a custom exchange name, declare and bind the queues
        if (!StringUtils.isEmpty(exchange.getExchangeName())) {
            ExchangeUtils.declare(channel, exchange);

            exchange.getDlq().ifPresent(dlqExchange -> {
                try {
                    ExchangeUtils.declare(channel, dlqExchange);

                    if (dlqExchange.isDeclareQueueWithSameName()) {
                        createAndBindQueue(channel, dlqExchange, dlqExchange.getDefaultQueue());

                        logger.with("dlq", dlqExchange).debug("Created dlq queue instance");
                    }
                }
                catch (IOException e) {
                    logger.error("Error declaring dql exchange", e);
                }
            });

            exchange.getRetryExchange().ifPresent(retryExchange -> {
                try {
                    ExchangeUtils.declare(channel, retryExchange);

                    createAndBindQueue(channel, retryExchange, queue.getRetryQueue());
                }
                catch (IOException e) {
                    logger.error("Error declaring retry exchange", e);
                }
            });

            createAndBindQueue(channel, exchange, queue);

            return bindQueueToRoutingKeys(channel, queue.getDeclaredName(), exchange, queue.getRoutingKeys());
        }
        else{
            createAndBindQueue(channel, exchange, queue);
        }

        return queue;
    }

    private static Queue createAndBindQueue(final Channel channel, final Exchange exchange, final Queue queue) throws IOException {
        String declaredQueueName = declareQueue(channel, exchange, queue);

        queue.setDeclaredName(declaredQueueName);

        bindQueueToRoutingKeys(channel, queue.getDeclaredName(), exchange, queue.getRoutingKeys());

        return queue;
    }

    /**
     * Declares a queue with RMQ
     *
     * @param channel
     * @param exchange
     * @param queue
     * @return
     * @throws IOException
     */
    public static String declareQueue(Channel channel, Exchange exchange, Queue queue) throws IOException {
        String queueName = queue.getName();

        final HashMap<String, Object> args = new HashMap<>();

        if (exchange.getDlq().isPresent()) {
            final Exchange dlqExchange = exchange.getDlq().get();

            args.put("x-dead-letter-exchange", dlqExchange.getExchangeName());
        }

        return channel.queueDeclare(queueName,
                                    queue.getOptions().getDurable(),
                                    queue.getOptions().getExclusive(),
                                    queue.getOptions().getAutoDelete(),
                                    args).getQueue();
    }

    /**
     * Initializes a topics endpoint. If the exchange doesn't have a queue name creates an adhoc queue
     * otherwise binds the topic to the supplied queue  and binds each routing key for the queue.
     * <p/>
     * If no routing key is specified, the routing key is the queue name
     *
     * @return
     * @throws IOException
     */
    public static Queue bindQueueToRoutingKeys(final Channel channel, String queueName, final Exchange exchange, final Optional<List<String>> optionalRoutes)
            throws IOException {
        final List<String> routingKeys = optionalRoutes.orElse(Arrays.asList(exchange.getExchangeName()));

        for (String routingKey : routingKeys) {
            channel.queueBind(
                    queueName,
                    exchange.getExchangeName(),
                    routingKey);
        }


        return new Queue(queueName, optionalRoutes);
    }

    /**
     * Set up correlation id and log the message came in
     *
     * @param rabbitRawMessage
     * @param newQueue
     * @return
     * @throws IOException
     */
    public static UUID setupMessageContext(final RabbitRawMessage rabbitRawMessage, final Exchange exchange, final Queue newQueue) throws IOException {
        UUID correlationId = UUID.fromString(rabbitRawMessage.getProperties().getCorrelationId());

        MDC.put(FilterAttributes.CORR_ID, correlationId);

        logger.debug(String.format("Got message message [%s] %s, exchange=%s, type=%s, queue=%s, routingKey=%s, listening_on=%s",
                                   correlationId,
                                   rabbitRawMessage.getEnvelope().getDeliveryTag(),
                                   exchange.getExchangeName(),
                                   exchange.getExchangeType().getExchangeTypeName(),
                                   newQueue.getName(),
                                   rabbitRawMessage.getEnvelope().getRoutingKey(),
                                   newQueue.getRoutingKeys()));

        return correlationId;
    }
}
