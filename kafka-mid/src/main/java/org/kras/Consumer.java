package org.kras;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new KafkaProperties().getProperties();
        String topic = "demo_java";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            final Thread thread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Detected consumer shut down...");
                    consumer.wakeup();
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            consumer.subscribe(List.of(topic));
            while (true) {
//                log.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key of message: {}, value: {}", record.key(), record.value());
                    log.info("Partition: {}, offset {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is shutting down... {} ", e.getMessage());
        } catch (Exception e) {
            log.error("Kafka consumer failed: {} ", e.getMessage());
        }
    }
}
