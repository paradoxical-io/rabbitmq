package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.results.MessageResult;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

final class EmptyMessagePromise<T> implements MessagePromise<T> {

    private static final EmptyMessagePromise<?> _instance = new EmptyMessagePromise();

    @SuppressWarnings("unchecked") // ok because this type doesn't do anything.
    public static <TMessage> EmptyMessagePromise<TMessage> instance() {
        return (EmptyMessagePromise<TMessage>) _instance;
    }

    private EmptyMessagePromise() { }

    @Override public void complete(final MessageResult result) { }

    @Override public Optional<Message<T>> getMessage() throws ExecutionException, InterruptedException {
        return Optional.empty();
    }
}
