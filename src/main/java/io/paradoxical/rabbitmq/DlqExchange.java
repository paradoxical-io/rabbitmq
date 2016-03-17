package io.paradoxical.rabbitmq;

public class DlqExchange extends Exchange {

    private final Queue defaultQueue;

    public DlqExchange(final String exchangeName) {
        super(Type.Fanout, exchangeName);
        
        if(exchangeName.endsWith("dlq")){
            defaultQueue = Queue.valueOf(exchangeName).withRoute(RoutingConstants.ONE_OR_MORE_SUB_STRING);
        }
        else{
            defaultQueue = Queue.valueOf(exchangeName + ".dlq").withRoute(RoutingConstants.ONE_OR_MORE_SUB_STRING);
        }
    }

    @Override
    public Queue getDefaultQueue() {
        return defaultQueue;
    }
}
