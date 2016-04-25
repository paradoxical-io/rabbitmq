package io.paradoxical.rabbitmq.tests;

import com.godaddy.logging.LoggerFactory;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerException;
import io.paradoxical.Container;
import io.paradoxical.DockerClientConfig;
import io.paradoxical.DockerCreator;
import io.paradoxical.rabbitmq.Exchange;
import io.paradoxical.rabbitmq.Queue;
import io.paradoxical.rabbitmq.QueueConfiguration;
import io.paradoxical.rabbitmq.connectionManagment.ChannelOptions;
import io.paradoxical.rabbitmq.connectionManagment.ChannelProvider;
import io.paradoxical.rabbitmq.connectionManagment.Host;
import io.paradoxical.rabbitmq.connectionManagment.SimpleChannelProvider;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class TestBase {
    private static final com.godaddy.logging.Logger logger = LoggerFactory.getLogger(TestBase.class);

    protected static final Random random = new Random();

    protected PodamFactory fixture = new PodamFactoryImpl();

    @Rule
    public TestName name = new TestName();

    @Before
    public void logTest() {
        System.out.println(name.getMethodName());
    }

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
    public static void teardown(){
        if(rabbitContainer != null){
            rabbitContainer.close();
        }
    }


    static {
        Logger rootLogger = Logger.getRootLogger();

        final String environmentLogLevel = System.getenv("LOG_LEVEL");

        rootLogger.setLevel(environmentLogLevel != null ? Level.toLevel(environmentLogLevel) : Level.INFO);

        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

        rootLogger.addAppender(new ConsoleAppender(layout));
    }

    protected Host getTestHost() {
        return new Host(URI.create(String.format("amqp://%s:%s",
                                                 rabbitContainer.getDockerHost(), rabbitContainer.getTargetPortToHostPortLookup().get(5672))));
    }

    protected void cleanup(Exchange exchange) {
        try {
            getTestChannelProvider().getChannel().exchangeDelete(exchange.getExchangeName());
        }
        catch (IOException e) {
            logger.error(e, "Error cleaning exchange");
        }
    }

    protected void cleanup(Queue queue) {
        try {
            getTestChannelProvider().getChannel().queueDelete(queue.getName());
        }
        catch (IOException e) {
            logger.error(e, "Error cleaning queue");
        }
    }

    protected ChannelProvider getTestChannelProvider() {
        return getTestChannelProvider(ChannelOptions.Default);
    }

    protected ChannelProvider getTestChannelProvider(ChannelOptions options) {
        try {
            return new SimpleChannelProvider(getTestHost(), options);
        }
        catch (Exception ex) {
            logger.error(ex, "Error getting provider");

            return null;
        }
    }

    protected QueueConfiguration autoDelete(final QueueConfiguration queueConfiguration) {
        queueConfiguration.getExchange().getOptions().setAutoDelete(true);

        queueConfiguration.getQueues().forEach(this::autoDelete);

        return queueConfiguration;
    }

    protected Queue autoDelete(Queue queue) {
        queue.getOptions().setAutoDelete(true);

        return queue;
    }

    protected void cleanup(final QueueConfiguration queueConfiguration) {
        cleanup(queueConfiguration.getExchange());

        queueConfiguration.getQueues().forEach(this::cleanup);
    }
}
