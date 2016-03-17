package io.paradoxical.rabbitmq;

import lombok.Data;

@Data
public class Message<T> {
    private T item;
    private Boolean isDlq;
    private Boolean isTtlExpired;
    private Boolean atLeastOnceDelivery;
    private RabbitRawMessage rabbitRawMessage;
}
