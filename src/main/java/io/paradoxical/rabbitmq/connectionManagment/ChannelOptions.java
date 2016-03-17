package io.paradoxical.rabbitmq.connectionManagment;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ChannelOptions {

    public static final Integer NO_MAX_RETRY_ATTEMPTS_VALUE = -1;

    private final Integer prefetchCount;

    private final Integer heartbeatIntervalInSec;

    private final Integer retryIntervalSeconds;

    private final Integer maxRetryAttempts;

    public static final ChannelOptions Default = ChannelOptions.builder()
                                                               .prefetchCount(8)
                                                               .heartbeatIntervalInSec(30)
                                                               .retryIntervalSeconds(3)
                                                               .maxRetryAttempts(NO_MAX_RETRY_ATTEMPTS_VALUE)
                                                               .build();

    public ChannelOptions(
            final Integer prefetchCount,
            final Integer heartbeatIntervalInSec,
            final Integer retryBackoffIntervalSeconds,
            final Integer maxRetryAttempts) {
        this.prefetchCount = prefetchCount == null ? Default.getPrefetchCount() : prefetchCount;

        this.heartbeatIntervalInSec = heartbeatIntervalInSec == null ? Default.getHeartbeatIntervalInSec() : heartbeatIntervalInSec;

        this.retryIntervalSeconds = retryBackoffIntervalSeconds == null ? Default.getRetryIntervalSeconds() : retryBackoffIntervalSeconds;

        this.maxRetryAttempts = maxRetryAttempts == null ? Default.getMaxRetryAttempts() : maxRetryAttempts;
    }
}
