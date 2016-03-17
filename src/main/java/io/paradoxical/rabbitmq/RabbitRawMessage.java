package io.paradoxical.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import lombok.Data;

@Data
public class RabbitRawMessage {
    final String consumerTag;
    final Envelope envelope;
    final AMQP.BasicProperties properties;
    final byte[] body;
    final Exchange exchange;
}
