package io.paradoxical.rabbitmq;

import lombok.Data;

@Data
public class ExchangeOptions {
    private Boolean durable = true;
    private Boolean autoDelete = false;
}
