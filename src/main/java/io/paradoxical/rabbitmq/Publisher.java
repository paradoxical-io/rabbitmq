package io.paradoxical.rabbitmq;


import java.io.IOException;

public interface Publisher<T>  {
    default void publish(T item) throws IOException{
        publish(item, null);
    }

    void publish(T item, PublisherOptions options) throws IOException;
}
