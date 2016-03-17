package io.paradoxical.rabbitmq;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

public class QueueConfiguration {
    @Getter
    private final Exchange exchange;

    @Getter
    private final List<Queue> queues;

    public QueueConfiguration(final Exchange exchange, final List<Queue> queues) {
        this.exchange = exchange;
        this.queues = queues;
    }

    public QueueConfiguration(final Exchange exchange, final Queue... queues) {
        this.exchange = exchange;
        this.queues = Arrays.asList(queues);
    }

    public QueueConfiguration(final Exchange exchange) {
        this.exchange = exchange;
        this.queues = Arrays.asList(Queue.valueOf(exchange.getExchangeName()));
    }
}

