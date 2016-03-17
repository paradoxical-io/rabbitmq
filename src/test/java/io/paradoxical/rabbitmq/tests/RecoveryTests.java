package io.paradoxical.rabbitmq.tests;

import io.paradoxical.rabbitmq.tests.categories.SlowTests;
import io.paradoxical.rabbitmq.tests.data.Data;
import io.paradoxical.rabbitmq.tests.data.DataListenerAsync;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.Publisher;
import io.paradoxical.rabbitmq.PublisherExchange;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelOptions;
import io.paradoxical.rabbitmq.connectionManagment.Host;
import io.paradoxical.rabbitmq.connectionManagment.SimpleChannelProvider;
import io.paradoxical.rabbitmq.queues.QueuePublisher;
import com.godaddy.logging.Logger;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import lombok.Cleanup;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.godaddy.logging.LoggerFactory.getLogger;
import static com.spotify.docker.client.DockerClient.LogsParam.follow;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static org.junit.Assert.assertEquals;

@Category(SlowTests.class)
public class RecoveryTests extends TestBase {
    public static final String DOCKER_MACHINE_SERVICE_URL = "https://192.168.99.100:2376";

    private static final Logger logger = getLogger(RecoveryTests.class);

    @Rule
    public Timeout timeout = new Timeout((int) Duration.ofMinutes(2).toMillis());

    @Test
    public void test_recovers_socket_closes() throws Exception {
        @Cleanup final RabbitDocker client = getClient("rabbitmq:3-management", "5672", "Server startup complete");

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
        client.getClient().stopContainer(client.getRunningContainer().id(), 10);

        logger.info("Waiting while recovery");

        // wait 10 seconds
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        logger.info("Starting container");

        // start the container up again
        client.getClient().startContainer(client.getRunningContainer().id());

        logger.info("Publishing message");

        publisher.publish(d);

        Thread.sleep(500);

        logger.info("Validating");

        // make sure the listener reconnected
        assertEquals(d, listener.getItem());
    }

    private RabbitDocker getClient(String imageName, String port, String waitForLog) throws DockerCertificateException, DockerException, InterruptedException {

        Map<String, List<PortBinding>> portBindings = new HashMap<String, List<PortBinding>>() {{
            put(port, Collections.singletonList(PortBinding.of("0.0.0.0", random.nextInt(30000) + 15000)));
        }};

        HostConfig hostConfig = HostConfig.builder()
                                          .portBindings(portBindings)
                                          .build();

        ContainerConfig.Builder configBuilder = ContainerConfig.builder()
                                                               .hostConfig(hostConfig)
                                                               .image(imageName)
                                                               .networkDisabled(false)
                                                               .exposedPorts(Arrays.asList(port).toArray(new String[]{}));

        final ContainerConfig container = configBuilder.build();

        final DockerClient client = createDockerClient();

        client.pull(configBuilder.image());

        final ContainerCreation createdContainer = client.createContainer(container);

        client.startContainer(createdContainer.id());

        if (!StringUtils.isEmpty(waitForLog)) {
            waitForLogInContainer(createdContainer, client, waitForLog);
        }

        final ContainerInfo containerInfo = client.inspectContainer(createdContainer.id());

        final String hostPort = containerInfo.networkSettings().ports().get(port + "/tcp").get(0).hostPort();

        return new RabbitDocker(client, createdContainer, hostPort);
    }

    private void waitForLogInContainer(final ContainerCreation createdContainer, final DockerClient client, final String waitForLog)
            throws DockerException, InterruptedException {

        LogStream logs = client.logs(createdContainer.id(), follow(), stdout());
        String log;
        do {
            LogMessage logMessage = logs.next();
            ByteBuffer buffer = logMessage.content();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            log = new String(bytes);
        } while (!log.contains(waitForLog));
    }

    private DockerClient createDockerClient() {
        if (isUnix() || System.getenv("DOCKER_HOST") != null) {
            try {
                return DefaultDockerClient.fromEnv().build();
            }
            catch (DockerCertificateException e) {
                System.err.println(e.getMessage());
            }
        }

        DockerCertificates dockerCertificates = null;
        try {
            String userHome = System.getProperty("user.home");
            dockerCertificates = new DockerCertificates(Paths.get(userHome, ".docker/machine/certs"));
        }
        catch (DockerCertificateException e) {
            System.err.println(e.getMessage());
        }
        return DefaultDockerClient.builder()
                                  .uri(URI.create(DOCKER_MACHINE_SERVICE_URL))
                                  .dockerCertificates(dockerCertificates)
                                  .build();
    }

    private static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.contains("nix") || os.contains("nux") || os.contains("aix");
    }

    @Value
    class RabbitDocker implements AutoCloseable {
        DockerClient client;
        ContainerCreation runningContainer;
        String hostPort;

        @Override
        public void close() throws Exception {
            client.stopContainer(runningContainer.id(), 10);
        }
    }
}
