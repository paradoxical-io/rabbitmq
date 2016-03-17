package io.paradoxical.rabbitmq;

import lombok.Data;

@Data
public class QueueOptions {
    private Boolean durable = true;
    private Boolean autoDelete = false;
    private Boolean exclusive = false;
}
