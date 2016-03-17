package io.paradoxical.rabbitmq.tests;

import io.paradoxical.rabbitmq.connectionManagment.ChannelOptions;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ChannelConfigTests {
    @Test
    public void test_defaults() {
        final ChannelOptions build = ChannelOptions.builder().build();

        assertThat(build).isEqualTo(ChannelOptions.Default);
    }
}
