package io.paradoxical.rabbitmq;

import lombok.Data;

@Data
public class PublisherExchange {
    private final Exchange exchange;
    private final String route;

    public static PublisherExchange valueOf(Exchange exchange){
        return new PublisherExchange(exchange, exchange.getExchangeName());
    }
}
