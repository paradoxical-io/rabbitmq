rabbitmq
========================


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
with a lot of channel/exchange/etc declaration.  We wanted a simple invokeable method that just _gives you events_. On 
top of that, we wanted

- Typed listeners
- Typed publishers
- Retry semantics 
- Easy DLQ setup
- Metrics
- Fault tolerant connections out of the box

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

### Listener metrics

Tying in io.dropwizard.metrics, listeners can log statistics such as in flight messages, and message process timing information 
granular by polymorphic event type.  For example, you can view information about an event heirarchy of:

```
class Data

class Child extends Data

class Child2 extends Data
```

Such that you can view 

- All event timing information
- Event timing for only `Child`
- Event timing for only `Child2`

You can also add what we call `metric groups`, which allow you to generate cross sectional denormalized metrics for easier
graphite reporting.

For example, lets say you have a listener who is configured to listen on credit card payment processing for a particular entity, 
like Bank Of America. There may be several kinds of listeners for payment processors running: Credit Card, Check, Bitcoin, whatever,
and all of those are by banking institutions: Bank Of America, USAA, etc.

You may want to be able to ask the question _how many total payment events are being processed_? And you may want to ask 
_how many credit cards are being processed by Bank Of America`? _How many events has USAA processed_?

This is totally doable but requires you to emit all these events yourself. Instead by passing in a metric group and a 
metric registry to the listener, the event base class can handle all this for you:

```
final List<String> metricGroups = Arrays.asList("bank", "creditcard", "bank.creditcard", "creditcard.bank");

final Exchange exchange = new Exchange("payment-exchange");

MetricRegistry registry = new MetricRegistry();

ListenerOptions listenerOptions = ListenerOptions.builder()
                                                 .metricRegistry(registry)
                                                 .metricGroups(metricGroups)
                                                 .build();
                                                 
DataListener<PaymentEvent> listener = 
        new DataListener<>(channelProvider, 
                           new QueueConfiguration(exchange, Queue.valueOf("USAA"),
                           listenerOptions);

listener.start();
```

You'll now get events that are prefixed with 

```
bank
creditcard
bank.creditcard
creditcard.bank
```

Which can be analyzed in graphite

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

## Custom serializer

Listeners can define their serializer via the `ListenerOptions` listener argument, and publishers can provide
their serializer during instantiation.  This lets you create custom serialization semantics. By default, messages
are serialized as JSON.
                                           

