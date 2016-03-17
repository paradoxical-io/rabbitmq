package io.paradoxical.rabbitmq;

import io.paradoxical.rabbitmq.results.MessageResult;

public interface EventProcessor<T>{
    MessageResult handleMessage(Message<T> message);
}
