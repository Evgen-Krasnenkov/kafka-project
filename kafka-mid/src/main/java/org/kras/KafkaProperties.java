package org.kras;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class KafkaProperties extends Properties {
    private final Properties properties;

    public KafkaProperties() {
        this.properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "kafka-app");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
    }

    public final Properties getProperties() {
        return this.properties;
    }
}
