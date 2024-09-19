package com.alexrhee.starterproject;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class KafkaSimple {
    private LinkedBlockingDeque<String> messageQueue;

    public KafkaSimple() {
        // Required for concurrency
        messageQueue = new LinkedBlockingDeque<>();
    }

    public int Size()
    {
        return messageQueue.size();
    }

    public void produce(String message) {
        messageQueue.add(message);
        // System.out.println("Produced: " + message);
    }

    public String consume() {
        try {
            String element = this.messageQueue.poll(5, TimeUnit.SECONDS); // Waits up to 5 seconds for an element
            if (element != null) {
                // System.out.println("Consumed: " + element);
                return element;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread was interrupted");
        }

        return null;
    }

    public static void main(String[] args) {
        KafkaSimple kafkaSimple = new KafkaSimple();
        kafkaSimple.produce("Message 1");
        kafkaSimple.produce("Message 2");
        kafkaSimple.consume();
        kafkaSimple.consume();
        kafkaSimple.consume();
    }
}

