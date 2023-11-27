package org.kras.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static final RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        RestHighLevelClient restHighLevelClient = null;
        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort())));
        }
        return restHighLevelClient;
    }

    ;

    private static final KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "kafka-app");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) {
        final RestHighLevelClient openSearchClient = createOpenSearchClient();
        final KafkaConsumer<String, String> consumer = createKafkaConsumer();
        final Thread mainThread = Thread.currentThread();

        log.info("Detected a shutdown, let's exit by calling consumer.wakeup()....");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        });

        try (openSearchClient; consumer) {

            boolean isExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (isExists) {
                log.info("WikiMedia already exists");
            } else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia Index has been created!");
            }
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int count = records.count();
                log.info("Consumer received: {} Records", count);
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    //Unique ID of   String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    String extractId = extractId(record.value());
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(extractId);
//                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    bulkRequest.add(indexRequest);
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulk = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} Doc(s) into OpenSearch", bulk.getItems().length);
                }


                consumer.commitAsync();
                log.info("Offset have been committed!");
            }

        } catch (WakeupException e) {
            log.info("detected WakeUp call of consumer");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
            try {
                openSearchClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
