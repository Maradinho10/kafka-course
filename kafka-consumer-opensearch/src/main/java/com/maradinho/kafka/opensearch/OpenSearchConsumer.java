package com.maradinho.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

    private static final String WIKIMEDIA_INDEX = "wikimedia";

    private static final String TOPIC = "wikimedia.recentchange";

    private static final Boolean AUTO_COMMIT_ENABLED = true;

    public static void main(String[] args) throws IOException {
        // Create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        // Create our Kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Create OpenSearch index if it doesn't exist
        try (openSearchClient; consumer) {
            createIndexIfNotExists(openSearchClient);

            consumer.subscribe(Collections.singleton(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} record(s).", recordCount);
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Send record to OpenSearch
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(WIKIMEDIA_INDEX)
                                .source(record.value(), XContentType.JSON)
                                .id(id);

//                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                        log.info("Inserted one document into OpenSearch, id: {}", indexResponse.getId());

                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        log.error("An error occurred while inserting into OpenSearch", e);
                    }
                }

                executeBulkRequest(openSearchClient, bulkRequest);

                if (!AUTO_COMMIT_ENABLED)  {
                    // Commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            }
        }
    }

    private static void executeBulkRequest(RestHighLevelClient openSearchClient, BulkRequest bulkRequest) throws IOException {
        int bulkNumOfActions = bulkRequest.numberOfActions();
        if (bulkNumOfActions > 0) {
            long startTime = System.currentTimeMillis();
            log.info("Bulk inserting {} records", bulkNumOfActions);
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("Inserted {} records in {} milliseconds.", bulkResponse.getItems().length, System.currentTimeMillis() - startTime);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Exception while sleeping after bulk request processed.", e);
            }
        }
    }

    private static void createIndexIfNotExists(RestHighLevelClient openSearchClient) throws IOException {
        boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);

        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(WIKIMEDIA_INDEX);
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("Wikimedia index has been created.");
        } else {
            log.info("Wikimedia index already exists.");
        }
    }

    private static String extractId(String jsonString) {
        return JsonParser.parseString(jsonString).getAsJsonObject()
                .get("meta").getAsJsonObject()
                .get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED.toString());

        return new KafkaConsumer<>(properties);
    }


    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // Builds URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        // Extracts login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http"))
            );
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
