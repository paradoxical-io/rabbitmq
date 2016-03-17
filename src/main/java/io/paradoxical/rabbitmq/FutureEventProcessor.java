package io.paradoxical.rabbitmq;

import io.paradoxical.rabbitmq.results.MessageResult;

import java.util.concurrent.Future;

public interface FutureEventProcessor<T>{
    Future<MessageResult> handleMessage(Message<T> message);
}

