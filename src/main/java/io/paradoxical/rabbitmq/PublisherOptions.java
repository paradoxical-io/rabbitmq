package io.paradoxical.rabbitmq;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;

@Builder
@Data
public class PublisherOptions {
    private PublishingContext context = new PublishingContext();

    private Duration messageTtl;

    public static final PublisherOptions Default = PublisherOptions.builder()
                                                                   .context(new PublishingContext())
                                                                   .build();
}
