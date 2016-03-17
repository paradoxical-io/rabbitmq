package io.paradoxical.rabbitmq;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class RetryExchange extends Exchange{
    private final String exchangeName;
    private final RetryStrategy strategy;

    public RetryExchange(Exchange requeueExchangeTo, final String exchangeName, RetryStrategy strategy) {
        super(Type.Fanout, exchangeName);

        this.withDlq(requeueExchangeTo);

        this.exchangeName = exchangeName;

        this.strategy = strategy;
    }
}
