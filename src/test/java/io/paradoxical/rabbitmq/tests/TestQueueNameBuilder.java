package io.paradoxical.rabbitmq.tests;

import io.paradoxical.rabbitmq.QueueNameBuilder;
import io.paradoxical.rabbitmq.QueueNameException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestQueueNameBuilder {

    @Test
    public void test_queue_name_builder() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("inames  GoDaddy", 1, "Connector-Event");

        assertEquals("consumer.inames-godaddy.v1.connector-event", queueName);
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
        String queueName = QueueNameBuilder.buildQueueName("inames-godaddy", "semantic", 1, "connector-event");

        assertEquals("consumer.inames-godaddy-semantic.v1.connector-event", queueName);
    }

    @Test
    public void test_queue_name_builder_with_class() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("inames-godaddy", 1, TestQueueNameBuilder.class);

        assertEquals("consumer.inames-godaddy.v1.test-queue-name-builder", queueName);
    }

    @Test
    public void test_queue_name_builder_with_class_and_semantic() throws QueueNameException {
        String queueName = QueueNameBuilder.buildQueueName("inames-godaddy", "semantic", 1, TestQueueNameBuilder.class);

        assertEquals("consumer.inames-godaddy-semantic.v1.test-queue-name-builder", queueName);
    }

    @Test(expected = QueueNameException.class)
    public void test_queue_name_no_version() throws QueueNameException {
        QueueNameBuilder.buildQueueName("inames-godaddy", -1, "connector-event");
    }
}
