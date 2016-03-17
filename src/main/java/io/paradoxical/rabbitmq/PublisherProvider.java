package io.paradoxical.rabbitmq;

public interface PublisherProvider<T> {
    default RoutingPublisher<T> forExchange(Exchange exchange) {
        return routingKey -> forExchange(exchange, routingKey);
    }

    Publisher<T> forExchange(Exchange exchange, String route);

    default Publisher<T> toQueue(Exchange exchange) {
        return forExchange(exchange, exchange.getExchangeName());
    }
}

