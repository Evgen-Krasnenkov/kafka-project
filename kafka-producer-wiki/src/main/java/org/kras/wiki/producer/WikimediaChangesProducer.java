package org.kras.wiki.producer;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.launchdarkly.eventsource.EventHandler;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            String topic = "wikimedia.recentchange";
            EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();
            eventSource.start();
            TimeUnit.MINUTES.sleep(10);
        } catch (Exception e) {
            log.error("Producer problems: {}", e.getMessage());
        }
    }
}
