package io.paradoxical.rabbitmq.tests;

import com.godaddy.logging.Logger;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import io.paradoxical.Container;
import io.paradoxical.DockerClientConfig;
import io.paradoxical.DockerCreator;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.Publisher;
import io.paradoxical.rabbitmq.PublisherExchange;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelOptions;
import io.paradoxical.rabbitmq.connectionManagment.Host;
import io.paradoxical.rabbitmq.connectionManagment.SimpleChannelProvider;
import io.paradoxical.rabbitmq.queues.QueuePublisher;
import io.paradoxical.rabbitmq.tests.categories.SlowTests;
import io.paradoxical.rabbitmq.tests.data.Data;
import io.paradoxical.rabbitmq.tests.data.DataListenerAsync;
import lombok.Cleanup;
import lombok.Value;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;

import static com.godaddy.logging.LoggerFactory.getLogger;
import static org.junit.Assert.assertEquals;

@Category(SlowTests.class)
public class RecoveryTests extends TestBase {
    private static final Logger logger = getLogger(RecoveryTests.class);

    @Rule
    public Timeout timeout = new Timeout((int) Duration.ofMinutes(2).toMillis());

    private static Container rabbitContainer;

    @BeforeClass
    public static void setup() throws InterruptedException, DockerException, DockerCertificateException {
        DockerClientConfig config =
                DockerClientConfig.builder()
                                  .imageName("rabbitmq:management")
                                  .port(5672)
                                  .waitForLogLine("Server startup complete")
                                  .build();

        rabbitContainer = DockerCreator.build(config);
    }

    @AfterClass
    public static void teardown() {
        if (rabbitContainer != null) {
            rabbitContainer.close();
        }
    }

    @Test
    public void test_recovers_socket_closes() throws Exception {
        @Cleanup final RabbitDocker client = getClient("rabbitmq:3-management", "Server startup complete");

        final URI uri = URI.create("amqp://" + client.getClient().getHost() + ":" + client.getHostPort());

        final SimpleChannelProvider simpleChannelProvider = new SimpleChannelProvider(new Host(uri), ChannelOptions.Default);

        String queue = "test.testConnectsToDocker";

        Exchange exchange = new Exchange(queue);

        PublisherExchange publisherExchange = PublisherExchange.valueOf(exchange);

        Publisher<Data> publisher = new QueuePublisher<>(simpleChannelProvider, publisherExchange, UUID::randomUUID);

        DataListenerAsync listener = new DataListenerAsync(simpleChannelProvider, autoDelete(new QueueConfiguration(exchange)));

        listener.start();

        Data d = fixture.manufacturePojo(Data.class);

        logger.info("Stopping container");

        // stop rmq
        client.getClient().stopContainer(client.getRunningContainerId(), 10);

        logger.info("Waiting while recovery");

        // wait 10 seconds
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        logger.info("Starting container");

        // start the container up again
        client.getClient().startContainer(client.getRunningContainerId());

        logger.info("Publishing message");

        publisher.publish(d);

        Thread.sleep(500);

        logger.info("Validating");

        // make sure the listener reconnected
        assertEquals(d, listener.getItem());
    }

    private RabbitDocker getClient(String imageName, String waitForLog) throws DockerCertificateException, DockerException, InterruptedException {

        return new RabbitDocker(rabbitContainer.getClient(),
                                rabbitContainer.getContainerInfo().id(),
                                rabbitContainer.getTargetPortToHostPortLookup().get(5672));
    }

    @Value
    class RabbitDocker implements AutoCloseable {
        DockerClient client;
        String runningContainerId;
        int hostPort;

        @Override
        public void close() throws Exception {
            client.stopContainer(runningContainerId, 10);
        }
    }
}
