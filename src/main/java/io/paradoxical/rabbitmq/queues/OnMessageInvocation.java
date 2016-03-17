package io.paradoxical.rabbitmq.queues;

import io.paradoxical.rabbitmq.Message;
import io.paradoxical.rabbitmq.results.MessageResult;

import java.util.concurrent.Future;

public interface OnMessageInvocation<T> {
    Future<MessageResult> onMessageAsync(final Message<T> item);
}
