package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.results.MessageResult;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface MessagePromise<T> {
    void complete(MessageResult result);

    Optional<Message<T>> getMessage() throws ExecutionException, InterruptedException;
}