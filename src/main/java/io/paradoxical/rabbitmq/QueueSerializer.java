package io.paradoxical.rabbitmq;

public interface QueueSerializer {
    <T> T read(byte[] bytes, Class<T> clazz) throws Exception;

    <T> T read(String json, Class<T> clazz) throws Exception;

    <T> byte[] writeBytes(T item) throws Exception;

    <T> String writeString(T item) throws Exception;
}