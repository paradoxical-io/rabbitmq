package io.paradoxical.rabbitmq;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * In order to standardized the naming of our queues, All queue names should be built using the QueueNameBuilder.
 */
public class QueueNameBuilder {

    private QueueNameBuilder() {}

    /**
     * Builds queue name.
     * @return consumer.(queueName).v(version).(eventType)
     * @throws QueueNameException if queueName is null or empty, version isn't > 0, or eventType is null or empty.
     */
    public static String buildQueueName(String queueName, int version, Class<?> eventType) throws QueueNameException {
        eventTypePresent(eventType);

        return buildQueueName(queueName, version, camelCaseJoiner(eventType.getSimpleName()));
    }

    /**
     * Builds queue name with semantic.
     * @return consumer.(queueName-semantic).v(version).(eventType)
     * @throws QueueNameException if queueName is null or empty, version isn't > 0, or eventType is null or empty.
     */
    public static String buildQueueName(String queueName, String semantic, int version, Class<?> eventType) throws QueueNameException {
        return buildQueueName(queueName + "-" + semantic, version, eventType);
    }

    /**
     * Builds queue name with semantic.
     * @return consumer.(queueName-semantic).v(version).(eventType)
     * @throws QueueNameException if queueName is null or empty, version isn't > 0, or eventType is null or empty.
     */
    public static String buildQueueName(String queueName, String semantic, int version, String eventType) throws QueueNameException {
        return buildQueueName(queueName + "-" + semantic, version, eventType);
    }

    /**
     * Builds queue name.
     * @return consumer.(queueName).v(version).(eventType)
     * @throws QueueNameException if queueName is null or empty, version isn't > 0, or eventType is null or empty.
     */
    public static String buildQueueName(String queueName, int version, String eventType) throws QueueNameException {
        versionOneOrGreater(version);

        eventTypePresent(eventType);

        return new StringBuilder("consumer.").append(getQueueName(queueName))
                                             .append(".v")
                                             .append(version)
                                             .append(".")
                                             .append(eventType.toLowerCase())
                                             .toString();
    }

    /**
     * Builds test queue name.
     * @return consumer.test-(queueName)
     */
    public static String buildTestQueueName(String queueName) throws QueueNameException {
        return new StringBuilder("consumer.test-").append(getQueueName(queueName))
                                                  .toString();
    }

    /**
     * Builds Test Queue Name from Executing method.
     * @return consumer.test-(executing method name)
     */
    public static String buildTestQueueName() throws QueueNameException {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();

        return camelCaseJoiner(buildTestQueueName(ste[2].getMethodName()));
    }

    private static String getQueueName(String queueName) throws QueueNameException {
        if(Strings.isNullOrEmpty(queueName)) {
            throw new QueueNameException("Queue name is required.");
        }

        return queueName.replaceAll("\\s+", "-")
                        .toLowerCase();
    }

    private static void versionOneOrGreater(int version) throws QueueNameException {
        if(version <= 0) {
            throw new QueueNameException("Version was " + version + ". Version must be 1 or later.");
        }
    }

    private static void eventTypePresent(Class<?> eventType) throws QueueNameException {
        if(eventType == null) {
            throw new QueueNameException("Event type is required.");
        }
    }

    private static void eventTypePresent(String eventType) throws QueueNameException {
        if(Strings.isNullOrEmpty(eventType)) {
            throw new QueueNameException("Event type is required.");
        }
    }

    private static String camelCaseJoiner(String camelCase) {
        List<String> camelCaseSplit = Arrays.asList(camelCase.split("(?=[A-Z])"));

        return camelCaseSplit.stream().collect(Collectors.joining("-"));
    }
}
