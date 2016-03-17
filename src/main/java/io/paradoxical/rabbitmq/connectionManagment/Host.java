package io.paradoxical.rabbitmq.connectionManagment;

import lombok.Data;

import java.net.URI;

@Data
public class Host{
    private final URI uri;
}
