package org.kras.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.kras.stream.processor.BotCountStreamBuilder;
import org.kras.stream.processor.EventCountTimeseriesBuilder;
import org.kras.stream.processor.WebsiteCountStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamKafkaWikiMedia {
    private final static Logger log = LoggerFactory.getLogger(StreamKafkaWikiMedia.class.getSimpleName());
    private static final Properties properties;
    private static final String INPUT_TOPIC = "wikimedia.recentchange";
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(INPUT_TOPIC);
        BotCountStreamBuilder botCountStreamBuilder = new BotCountStreamBuilder(stream);
        botCountStreamBuilder.setup();
        WebsiteCountStreamBuilder websiteCountStreamBuilder = new WebsiteCountStreamBuilder(stream);
        websiteCountStreamBuilder.setup();
        EventCountTimeseriesBuilder eventCountTimeseriesBuilder = new EventCountTimeseriesBuilder(stream);
        eventCountTimeseriesBuilder.setup();
        final Topology topology = builder.build();
        log.info("Topology: {}", topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();

    }
}
