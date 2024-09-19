package com.alexrhee.starterproject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSimpleTest {

    private KafkaSimple kafkaSimple;

    @BeforeEach
    void setUp() {
        kafkaSimple = new KafkaSimple();
    }

    @Test
    void testProduceAndConsume() {
        // Test producing messages
        kafkaSimple.produce("Test Message 1");
        kafkaSimple.produce("Test Message 2");

        // Test consuming messages
        assertEquals("Test Message 1", kafkaSimple.consume());
        assertEquals("Test Message 2", kafkaSimple.consume());

        // Test consuming when no messages are left
        assertNull(kafkaSimple.consume());
    }

    @Test
    void testConsumeEmptyQueue() {
        // Test consuming from an empty queue
        assertNull(kafkaSimple.consume());
    }

    @Test
    void testProduceNullMessage() {
        // Test producing a null message
        kafkaSimple.produce(null);
        assertNull(kafkaSimple.consume());
    }

    @Test
    void testConcurrentProduceAndConsume() throws InterruptedException {
        int threadCount = 10;
        int messagesPerProducer = 100;
        int totalMessages = threadCount * messagesPerProducer;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount * 2);
        CountDownLatch latch = new CountDownLatch(threadCount * 2);

        // Producing messages concurrently
        for (int i = 0; i < threadCount; i++) {
            int threadNumber = i;
            executorService.submit(() -> {
                for (int j = 0; j < messagesPerProducer; j++) {
                    kafkaSimple.produce("Message from producer " + threadNumber + ": " + j);
                }
                latch.countDown();
            });
        }

        // Consuming messages concurrently
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < messagesPerProducer; j++) {
                    kafkaSimple.consume();
                }
                latch.countDown();
            });
        }

        // Wait for all tasks to complete
        latch.await(10, TimeUnit.SECONDS);

        // Check if queue is empty after all operations
        assertEquals(0, kafkaSimple.Size());;
        assertNull(kafkaSimple.consume());

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS), "Executor did not shut down in time");
    }

    @Test
    void testHighConcurrency() throws InterruptedException {
        int threadCount = 100;
        int messagesPerProducer = 100;
        int totalMessages = threadCount * messagesPerProducer;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount * 2);
        CountDownLatch latch = new CountDownLatch(threadCount * 2);

        // Producing messages concurrently
        for (int i = 0; i < threadCount; i++) {
            int threadNumber = i;
            executorService.submit(() -> {
                for (int j = 0; j < messagesPerProducer; j++) {
                    kafkaSimple.produce("Message from producer " + threadNumber + ": " + j);
                }
                latch.countDown();
            });
        }

        // Consuming messages concurrently
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < messagesPerProducer; j++) {
                    kafkaSimple.consume();
                }
                latch.countDown();
            });
        }

        // Wait for all tasks to complete
        latch.await(30, TimeUnit.SECONDS);

        // Check if queue is empty after all operations
        assertNull(kafkaSimple.consume());

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS), "Executor did not shut down in time");
    }
}
