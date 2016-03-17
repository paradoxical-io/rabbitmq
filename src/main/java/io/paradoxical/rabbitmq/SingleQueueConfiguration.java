package io.paradoxical.rabbitmq;

import lombok.Getter;
import lombok.Value;

@Value
public class SingleQueueConfiguration {
    @Getter
    private final Exchange exchange;

    @Getter
    private final Queue queue;
}
