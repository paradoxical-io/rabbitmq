package io.paradoxical.rabbitmq.results;

public enum MessageResult {
    /**
     * Acks the message
     */
    Ack,

    /**
     * Kills the message and does not redeliver
     */
    Nack,

    /**
     * Nacks or republishes up to the max configured retry amount
     */
    RequeueUntilMaxTries,

    /**
     * Nack and reqeueue no matter how many times its been requeued already
     *
     * Careful using this as it may result in a poison message that is always tossed around
     */
    Defer,

    /**
     * If a retry exchange is set up will republish the message to the retry exchange.
     */
    RetryLater
}
