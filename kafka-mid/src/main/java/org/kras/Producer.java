package org.kras;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello Kafka");
        Properties properties = new KafkaProperties().getProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {

            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = topic + i;
                String value = "hello kafka" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                log.info("Record: {}", record.key());
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (null == exception) {
                            log.info("Producer sent metadata: {}, key: {}", new LocalKafkaMetadata(metadata).toString(), key);
                        } else {
                            throw new RuntimeException("Problem with CallBack of producer!");
                        }
                    }
                });
                producer.flush();
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Problems with producer of kafka", e.getCause());
        }

    }
}
