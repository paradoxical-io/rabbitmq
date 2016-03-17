domains.rabbitmq
========================
[![Build Status](http://dom-jenkins.cloud.dev.phx3.gdg/job/Domains.RabbitMq/badge/icon)](http://dom-jenkins.cloud.dev.phx3.gdg/job/Domains.RabbitMq/)

This is an RMQ wrapper library that provides simpler RMQ access.

For example

```
public class DataListener extends QueueListenerSync<Data> {
    @Getter
    private Data item;

    public DataListener(
        final ChannelProvider channelProvider, 
        final QueueConfiguration info, 
        ListenerOptions options) 
        throws
          IOException,
          InterruptedException,
          NoSuchAlgorithmException,
          KeyManagementException,
          URISyntaxException {
        super(channelProvider, info, Data.class, options);
    }

    @Override
    public MessageResult onMessage(final Data item) {
        this.item = item;

        return MessageResult.Ack;
    }
}
```

The library supports 

- Ack 
- Nack (kill message)
- Reqeue (Nack with reschedule if not already delivered up to max times)
- RetryLater (Will attempt to re-publish the message to a delayed retry exchange)

Also included is nicer publisher support:

```
val publisher = new PublisherProviderImpl<>(getTestChannelProvider()).forExchange(exchange)
                                                                     .onRoute("foo");

publisher.publish(fixture.manufacturePojo(Data.class));
```

