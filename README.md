rabbitmq
========================
[![Build Status](https://travis-ci.org/paradoxical-io/rabbitmq.svg?branch=master)](https://travis-ci.org/paradoxical-io/rabbitmq)

This is an RMQ wrapper library that provides simpler RMQ access. 

#Installation

```
<dependency>
    <groupId>io.paradoxical</groupId>
    <artifactId>rabbitmq</artifactId>
    <version>1.0</version>
</dependency>
```

# Why another java RMQ library?

The java library provided by RMQ is full featured, but isn't well typed and requires you to intermix your event handling code
with a lot of channel/exchange/etc declaration.  We wanted a simple invokeable method that just _gives you events_

## Listeners

For example, below we have a queue listener that is typed, so events that this listener sits on should be of type `Data`.

We can enforce publishing events of this type with a strong typed publisher, and it will handle all the serialization for us.

```
public class DataListener extends QueueListenerSync<Data> {
    @Getter
    private Data item;

    public DataListener(
        final ChannelProvider channelProvider, 
        final QueueConfiguration info, 
        ListenerOptions options) {
        super(channelProvider, info, Data.class, options);
    }

    @Override
    public MessageResult onMessage(final Data item) {
        this.item = item;

        return MessageResult.Ack;
    }
}
```

Listeners are created with an instance of a `QueueConfiguration` class which tells the listener on what to listen to
given the 

- Exchange
- Queues
    - Which routes on which queue
  
For example:

```
Queue queue = Queue.valueOf("foo").withRoutes("v1.#.event", "v1.#.event2");

final Exchange exchange = new Exchange(Exchange.Type.Topic, "exchange");

DataListener listener = new DataListener(getTestChannelProvider(), new QueueConfiguration(exchange, queue));
```

Queues and exchanges also expose options to set

- Autodelete
- Exclusive
- Durability

Via strongly typed values.

We can also wire in DLQ semantics:

```
Exchange dlqExchange = Exchange.asDlq(getUnique("test.dlq"));

dlqExchange.setDeclareQueueWithSameName(true);

Exchange exchange = new Exchange(queue).withDlq(dlqExchange);
```

A DLQ exchange can auto delcare a queue with the same name as the DLQ name, which ensures that DLQ events don't go 
into the RMQ ether if there is nobody listening on it.

### Promise based consumers

Instead of having a synchronous worker listener, we can also create a promise based consumer.  This can be useful if you 
want to `pull` messages from RMQ instead of be `pushed` messages.

```
PromiseQueueConsumer<Data> promiseConsumer =
                new PromiseQueueConsumer<>(getTestChannelProvider(),
                                           new SingleQueueConfiguration(exchange, queue),
                                           Data.class);

// wait at most 10 milliseconds for a message                                           
MessagePromise<Data> promise = promiseConsumer.getNextMessage(Duration.ofMillis(20));
                                           
if(promise.getMessage().isPresent()){
    promise.complete(MessageResult.Ack);
}                                              
```
                                           

## Retry exchanges

RMQ supports message TTL's and as such you can create a retry queue. This can be nice if you want to retry messages a
few hours later.  To do that, create a retry exchange:

```
RetryStrategy retryStrategy = (attempts, item) -> Optional.of(Duration.ofSeconds(attempts * 2))

Exchange exchange = new Exchange(Exchange.Type.Topic, "exponential_retry_exchange").withRetryExchange(retrStrategy);
```

The retry exchange strategy lets you define how many times you want to republish and what is the duration to wait between events.

## Publishing

Also included is nicer publisher support.


### Topics

Below is an example with a topic exchange.

```
ChannelProvider provider = new SimpleChannelProvider(new Host(URI.create("amqp://...")));

Exchange exchange = new Exchange(Exchange.Type.Topic, "foo-exchange");

val publisher = new PublisherProviderImpl<>(provider).forExchange(exchange)
                                                     .onRoute("foo");

publisher.publish(new Data());
```

We can now control and ensure that we are publishing the correct serializable events to the right publisher, so there 
is no accidental publishing the wrong message to the wrong exchange (which can cause dead messages/poison messages if not careful)

### Direct

We can also publish to a direct exchange 

```
ChannelProvider provider = new SimpleChannelProvider(new Host(URI.create("amqp://...")));

Exchange exchange = new Exchange(Exchange.Type.Topic, "foo-exchange");

val publisher = new PublisherProviderImpl<>(provider).forQueue(exchange);

publisher.publish(new Data());
```

### Publishing semantics

The library supports 

- Ack 
- Nack (kill message)
- Reqeue (Nack with reschedule if not already delivered up to max times)
- Defer (Nack with reschedule ignoring max times
- RetryLater (Will attempt to re-publish the message to a delayed retry exchange)


## Retries

Reconnecting and retries are handled automatically by the `lyra` library, and is bundled automatically. Several 
options are exposed as part of the `ChannelOptions` class which is used to instantiate a channel provider:

