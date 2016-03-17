package io.paradoxical.rabbitmq.connectionManagment;

import com.godaddy.logging.Logger;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import static com.godaddy.logging.LoggerFactory.getLogger;

/**
 * Adds an abstraction over poolable amq channels
 */
public class SimpleChannelProvider implements ChannelProvider {
    private static final Logger logger = getLogger(SimpleChannelProvider.class);

    private final Connection connection;

    private final ChannelOptions channelOptions;

    public SimpleChannelProvider(Host host) throws
                                            NoSuchAlgorithmException,
                                            KeyManagementException,
                                            URISyntaxException,
                                            IOException {
        this(host, ChannelOptions.Default);
    }

    public SimpleChannelProvider(Host host, ChannelOptions channelOptions) throws
                                                                           NoSuchAlgorithmException,
                                                                           KeyManagementException,
                                                                           URISyntaxException,
                                                                           IOException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();

        connectionFactory.setUri(host.getUri());

        this.channelOptions = channelOptions;

        connection = setupConnection(connectionFactory.getUsername(),
                                     connectionFactory.getPassword(),
                                     connectionFactory.getVirtualHost(),
                                     connectionFactory.getHost(),
                                     connectionFactory.getPort(),
                                     channelOptions);
    }

    public SimpleChannelProvider(
            final String userName,
            final String password,
            final String virtualHost,
            Address[] contactPoints) throws
                                     NoSuchAlgorithmException,
                                     KeyManagementException,
                                     URISyntaxException,
                                     IOException {
        this(userName, password, virtualHost, contactPoints, ChannelOptions.Default);
    }

    public SimpleChannelProvider(
            final String userName,
            final String password,
            final String virtualHost,
            Address[] contactPoints,
            ChannelOptions channelOptions) throws
                                           NoSuchAlgorithmException,
                                           KeyManagementException,
                                           URISyntaxException,
                                           IOException {
        this.channelOptions = channelOptions;

        connection = setupConnection(userName, password, virtualHost, contactPoints, channelOptions);
    }

    public Channel getChannel() {
        try {
            Channel channel = connection.createChannel();
            channel.basicQos(this.channelOptions.getPrefetchCount());
            return channel;
        }
        catch (IOException e) {
            logger.error(e, "Error getting channel");

            return null;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private static Connection setupConnection(String userName, String password, String virtualHost, String host, int port, ChannelOptions channelOptions)
            throws IOException {
        return setupConnection(userName,
                               password,
                               virtualHost,
                               new Address[]{ (new Address(host, port)) },
                               channelOptions);
    }

    private static Connection setupConnection(String userName, String password, String virtualHost, Address[] contactPoints, ChannelOptions channelOptions)
            throws IOException {
        Config config = new Config().withRecoveryPolicy(RecoveryPolicies.recoverAlways()
                                                                        .withInterval(Duration.seconds(channelOptions.getRetryIntervalSeconds())))
                                    .withRetryPolicy(new RetryPolicy()
                                                             .withInterval(Duration.seconds(channelOptions.getRetryIntervalSeconds()))
                                                             .withMaxAttempts(channelOptions.getMaxRetryAttempts()));

        final ConnectionOptions connectionOptions = new ConnectionOptions().withAddresses(contactPoints)
                                                                           .withUsername(userName)
                                                                           .withPassword(password)
                                                                           .withVirtualHost(virtualHost)
                                                                           .withRequestedHeartbeat(Duration.seconds(channelOptions.getHeartbeatIntervalInSec()));

        connectionOptions.getConnectionFactory().setAutomaticRecoveryEnabled(true);

        try {
            return Connections.create(connectionOptions, config);
        }
        catch (TimeoutException e) {
            throw new IOException(e);
        }
    }
}
