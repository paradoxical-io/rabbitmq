package io.paradoxical.rabbitmq;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

public class Exchange {
    @Getter private final Type exchangeType;
    @Getter private final String exchangeName;
    @Getter private final ExchangeOptions options = new ExchangeOptions();
    @Getter private Optional<Exchange> dlq = Optional.empty();

    @Getter
    private final Queue defaultQueue;

    @Getter
    @Setter
    private boolean declareQueueWithSameName;

    @Getter private Optional<RetryExchange> retryExchange = Optional.empty();


    public Exchange(final Type exchange, String exchangeName) {
        this.exchangeType = exchange;
        this.exchangeName = exchangeName;

        defaultQueue = Queue.valueOf(getExchangeName()).withRoute(getExchangeName());
    }

    public Exchange(final String name) {
        this(Type.Direct, name);
    }

    public Exchange withDlq(Exchange dlq) {
        this.dlq = Optional.of(dlq);

        return this;
    }

    public Exchange withRetryExchange(RetryStrategy strategy) {
        this.retryExchange = Optional.of(new RetryExchange(this, exchangeName + ".retry", strategy));

        return this;
    }

    public static DlqExchange asDlq(final String name) {
        return new DlqExchange(name);
    }

    public enum Type {
        Direct("direct"),
        Fanout("fanout"),
        Topic("topic"),
        Headers("headers");

        @Getter
        private final String exchangeTypeName;

        Type(final String exchangeName) {

            this.exchangeTypeName = exchangeName;
        }

        @Override public String toString() {
            return "Type{" +
                   "exchangeTypeName='" + exchangeTypeName + '\'' +
                   '}';
        }
    }
}
