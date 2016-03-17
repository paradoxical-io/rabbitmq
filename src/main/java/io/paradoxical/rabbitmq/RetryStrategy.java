package io.paradoxical.rabbitmq;

import java.time.Duration;
import java.util.Optional;

public interface RetryStrategy{
    Optional<Duration> nextRetry(int previousAttempts, Object item);
}
