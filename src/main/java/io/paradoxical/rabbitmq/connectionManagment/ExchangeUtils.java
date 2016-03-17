package io.paradoxical.rabbitmq.connectionManagment;

import io.paradoxical.rabbitmq.Exchange;
import com.rabbitmq.client.Channel;

import java.io.IOException;

public class ExchangeUtils {

    public static void declare(final Channel channel, final Exchange exchange) throws IOException {
        channel.exchangeDeclare(exchange.getExchangeName(),
                                exchange.getExchangeType().getExchangeTypeName(),
                                exchange.getOptions().getDurable(),
                                exchange.getOptions().getAutoDelete(),
                                null);
    }
}
