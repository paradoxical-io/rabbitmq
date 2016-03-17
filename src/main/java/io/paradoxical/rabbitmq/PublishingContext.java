package io.paradoxical.rabbitmq;

import lombok.Data;

@Data
public class PublishingContext{
    int previousPublishes;
}
