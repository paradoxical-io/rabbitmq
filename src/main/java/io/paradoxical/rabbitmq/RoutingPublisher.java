package io.paradoxical.rabbitmq;

@FunctionalInterface
public interface RoutingPublisher<T> {
    Publisher<T> onRoute(String route);
}

