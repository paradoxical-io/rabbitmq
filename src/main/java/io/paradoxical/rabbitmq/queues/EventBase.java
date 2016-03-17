package io.paradoxical.rabbitmq.queues;

import lombok.Data;

import java.util.UUID;

@Data
public abstract class EventBase {
    UUID correlationId;
}
