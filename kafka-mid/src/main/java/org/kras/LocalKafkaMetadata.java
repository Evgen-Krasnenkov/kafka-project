package org.kras;

import org.apache.kafka.clients.producer.RecordMetadata;

public class LocalKafkaMetadata {
    private final RecordMetadata metadata;

    public LocalKafkaMetadata(RecordMetadata metadata) {
        this.metadata = metadata;
    }


    public String toString() {
        return "RecordMetadata(" +
                "topic=" + metadata.topic() +
                ", partition=" + metadata.partition() +
                ", offset=" + metadata.offset() +
                ", timestamp=" + metadata.timestamp() +
                ")";
    }
}
