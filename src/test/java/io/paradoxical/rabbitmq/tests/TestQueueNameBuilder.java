package io.paradoxical.rabbitmq.tests;

import io.paradoxical.rabbitmq.QueueNameBuilder;
import io.paradoxical.rabbitmq.QueueNameException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestQueueNameBuilder {

    @Test
    public void test_queue_name_builder() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("worker  paradoxical", 1, "plugin-Event");

        assertEquals("consumer.worker-paradoxical.v1.plugin-event", queueName);
    }

    @Test
    public void test_test_queue_name_builder() throws QueueNameException {
        String testQueueName = QueueNameBuilder.buildTestQueueName("testName");

        assertEquals("consumer.test-testname", testQueueName);
    }

    @Test
    public void test_test_queue_name_builder_with_method() throws QueueNameException {
        String testQueueName = QueueNameBuilder.buildTestQueueName();

        assertEquals("consumer.test-test_test_queue_name_builder_with_method", testQueueName);
    }

    @Test
    public void test_queue_name_builder_with_semantic() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("worker-paradoxical", "semantic", 1, "plugin-event");

        assertEquals("consumer.worker-paradoxical-semantic.v1.plugin-event", queueName);
    }

    @Test
    public void test_queue_name_builder_with_class() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("worker-paradoxical", 1, TestQueueNameBuilder.class);

        assertEquals("consumer.worker-paradoxical.v1.test-queue-name-builder", queueName);
    }

    @Test
    public void test_queue_name_builder_with_class_and_semantic() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("worker-paradoxical", "semantic", 1, TestQueueNameBuilder.class);

        assertEquals("consumer.worker-paradoxical-semantic.v1.test-queue-name-builder", queueName);
    }

    @Test(expected = QueueNameException.class)
    public void test_queue_name_no_version() throws QueueNameException {
        QueueNameBuilder.buildQueueName("worker-paradoxical", -1, "plugin-event");
    }
}
