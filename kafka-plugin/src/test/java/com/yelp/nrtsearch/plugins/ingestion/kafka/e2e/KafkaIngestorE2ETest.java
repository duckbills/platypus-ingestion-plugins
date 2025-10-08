package com.yelp.nrtsearch.plugins.ingestion.kafka.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Comprehensive E2E test for Kafka ingestion plugin.
 *
 * <p>Test Flow: 1. Start Kafka + Schema Registry (Testcontainers) 2. Start real nrtSearch server
 * with plugin loading 3. Create index and register schema 4. Publish Avro messages to Kafka 5. Wait
 * for ingestion to index documents 6. Query nrtSearch to verify data
 */
public class KafkaIngestorE2ETest extends com.yelp.nrtsearch.test_utils.NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIngestorE2ETest.class);

  static final String TEST_TOPIC = "test-documents";
  static final String INDEX_NAME = "test-index";
  static final String CONSUMER_GROUP_BASE = "test-consumer-group";
  static final String S3_BUCKET_NAME = "test-bucket";

  public KafkaIngestorE2ETest() throws IOException {
    super();
  }

  private static final String PLUGIN_S3_KEY =
      "nrtsearch/plugins/nrtsearch-plugin-kafka-0.1.0-SNAPSHOT.zip";

  @BeforeClass
  public static void addPluginToS3() throws Exception {
    // Build the plugin distribution first
    buildPluginDistribution();

    // Upload the plugin zip to S3
    String projectRoot = System.getProperty("user.dir");
    String pluginZipPath =
        projectRoot + "/build/distributions/nrtsearch-plugin-kafka-0.1.0-SNAPSHOT.zip";

    getS3Client().putObject(getS3BucketName(), PLUGIN_S3_KEY, new java.io.File(pluginZipPath));
  }

  @Override
  protected List<String> getPlugins() {
    String path = String.format("s3://%s/%s", getS3BucketName(), PLUGIN_S3_KEY);
    return List.of(path);
  }

  // Testcontainers
  static Network network;
  static KafkaContainer kafka;
  static GenericContainer<?> schemaRegistry;

  // Test data
  static final String AVRO_SCHEMA_JSON =
      """
        {
          "type": "record",
          "name": "Document",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "title", "type": "string"},
            {"name": "content", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "rating", "type": "double"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "metadata", "type": {
              "type": "record",
              "name": "Metadata",
              "fields": [
                {"name": "author", "type": "string"},
                {"name": "publishDate", "type": "string"}
              ]
            }}
          ]
        }
        """;

  private static void buildPluginDistribution() throws Exception {
    String projectRoot = System.getProperty("user.dir");
    ProcessBuilder pb = new ProcessBuilder("./gradlew", ":nrtsearch-plugin-kafka:distZip");
    pb.directory(
        new java.io.File(projectRoot).getParentFile()); // Go up to nrtsearch-ingestion-plugins
    pb.inheritIO(); // Show build output
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Failed to build plugin distribution, exit code: " + exitCode);
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    startInfrastructure();
    publishTestData();
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config); // Get the defaults including plugin setup

    // Add plugin configuration for Kafka
    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> kafkaConfigs = new HashMap<>();

    // Generate unique consumer group ID for each test instance
    String uniqueGroupId = CONSUMER_GROUP_BASE + "-" + System.nanoTime();

    kafkaConfigs.put("bootstrapServers", kafka.getBootstrapServers());
    kafkaConfigs.put("topic", TEST_TOPIC);
    kafkaConfigs.put("groupId", uniqueGroupId);
    kafkaConfigs.put("indexName", INDEX_NAME);
    kafkaConfigs.put("schemaRegistryUrl", "http://localhost:" + schemaRegistry.getMappedPort(8081));
    kafkaConfigs.put("autoOffsetReset", "earliest");
    kafkaConfigs.put("batchSize", 10);

    ingestionConfigs.put("kafka", kafkaConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  static void startInfrastructure() {
    LOGGER.info("Starting Kafka infrastructure...");

    network = Network.newNetwork();

    // Start Kafka
    kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
            .withNetwork(network)
            .withNetworkAliases("kafka");
    kafka.start();

    // Start Schema Registry
    schemaRegistry =
        new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.1"))
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withExposedPorts(8081);
    schemaRegistry.start();

    LOGGER.info(
        "Kafka: {}, Schema Registry: {}",
        kafka.getBootstrapServers(),
        "http://localhost:" + schemaRegistry.getMappedPort(8081));
  }

  private void setupIndexAndSchema() {
    LOGGER.info("Setting up index and registering fields...");

    // Wait a bit for the server to fully start
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Create index
    CreateIndexRequest createRequest =
        CreateIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().createIndex(createRequest);

    // Register fields matching Avro schema
    FieldDefRequest.Builder fieldDefBuilder = FieldDefRequest.newBuilder().setIndexName(INDEX_NAME);

    // Basic fields
    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("id")
            .setType(FieldType.ATOM)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("title")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .setStore(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("content")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .setStore(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("category")
            .setType(FieldType.ATOM)
            .setSearch(true)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("rating")
            .setType(FieldType.DOUBLE)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    // Array field
    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("tags")
            .setType(FieldType.ATOM)
            .setMultiValued(true)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    // Nested fields (flattened with underscores - dots not allowed in field names)
    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("metadata_author")
            .setType(FieldType.ATOM)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("metadata_publishDate")
            .setType(FieldType.ATOM)
            .setStoreDocValues(true)
            .setStore(true)
            .build());

    getClient().getBlockingStub().registerFields(fieldDefBuilder.build());

    // Start index
    StartIndexRequest startRequest =
        StartIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().startIndex(startRequest);

    LOGGER.info("Index setup complete");
  }

  static void publishTestData() throws Exception {
    LOGGER.info("Publishing test data to Kafka...");

    // Setup Kafka producer
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", "http://localhost:" + schemaRegistry.getMappedPort(8081));

    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);

    try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
      // Create test documents
      List<GenericRecord> testDocs = createTestDocuments(schema);

      for (GenericRecord doc : testDocs) {
        String key = doc.get("id").toString();
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TEST_TOPIC, key, doc);
        producer.send(record);
        LOGGER.debug("Sent document: {}", key);
      }

      producer.flush();
      LOGGER.info("Published {} test documents", testDocs.size());
    }
  }

  static List<GenericRecord> createTestDocuments(Schema schema) {
    List<GenericRecord> docs = new ArrayList<>();

    // Document 1
    GenericRecord doc1 = new GenericData.Record(schema);
    doc1.put("id", "doc1");
    doc1.put("title", "Machine Learning Basics");
    doc1.put("content", "Introduction to neural networks and deep learning concepts");
    doc1.put("category", "technology");
    doc1.put("rating", 4.5);
    doc1.put("tags", Arrays.asList("ml", "ai", "tutorial"));

    GenericRecord metadata1 = new GenericData.Record(schema.getField("metadata").schema());
    metadata1.put("author", "Alice Smith");
    metadata1.put("publishDate", "2024-01-15");
    doc1.put("metadata", metadata1);
    docs.add(doc1);

    // Document 2
    GenericRecord doc2 = new GenericData.Record(schema);
    doc2.put("id", "doc2");
    doc2.put("title", "Cooking Pasta Perfectly");
    doc2.put("content", "Tips and tricks for making restaurant-quality pasta at home");
    doc2.put("category", "cooking");
    doc2.put("rating", 4.8);
    doc2.put("tags", Arrays.asList("food", "recipe", "italian"));

    GenericRecord metadata2 = new GenericData.Record(schema.getField("metadata").schema());
    metadata2.put("author", "Chef Mario");
    metadata2.put("publishDate", "2024-01-20");
    doc2.put("metadata", metadata2);
    docs.add(doc2);

    // Document 3
    GenericRecord doc3 = new GenericData.Record(schema);
    doc3.put("id", "doc3");
    doc3.put("title", "Travel Guide: Tokyo");
    doc3.put("content", "Best places to visit and authentic food experiences in Tokyo");
    doc3.put("category", "travel");
    doc3.put("rating", 4.2);
    doc3.put("tags", Arrays.asList("travel", "japan", "guide"));

    GenericRecord metadata3 = new GenericData.Record(schema.getField("metadata").schema());
    metadata3.put("author", "Travel Blogger");
    metadata3.put("publishDate", "2024-01-25");
    doc3.put("metadata", metadata3);
    docs.add(doc3);

    return docs;
  }

  @Test
  public void testEndToEndKafkaIngestion() {
    LOGGER.info("Testing end-to-end Kafka ingestion...");

    // Setup index and schema
    setupIndexAndSchema();

    // Wait for documents to be ingested and indexed
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              SearchRequest searchRequest =
                  SearchRequest.newBuilder()
                      .setIndexName(INDEX_NAME)
                      .setStartHit(0)
                      .setTopHits(10)
                      .setQuery(
                          Query.newBuilder()
                              .setMatchAllQuery(MatchAllQuery.newBuilder().build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .addRetrieveFields("category")
                      .addRetrieveFields("rating")
                      .addRetrieveFields("tags")
                      .addRetrieveFields("metadata_author")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);
              LOGGER.info("Search returned {} hits", response.getTotalHits().getValue());

              // Verify we got all 3 documents
              assertEquals(3, response.getTotalHits().getValue());

              // Verify document content
              Map<String, SearchResponse.Hit> hitsByKey = new HashMap<>();
              for (SearchResponse.Hit hit : response.getHitsList()) {
                String id = getFieldValue(hit, "id");
                hitsByKey.put(id, hit);
              }

              // Verify doc1
              SearchResponse.Hit doc1Hit = hitsByKey.get("doc1");
              assertNotNull(doc1Hit);
              assertEquals("Machine Learning Basics", getFieldValue(doc1Hit, "title"));
              assertEquals("technology", getFieldValue(doc1Hit, "category"));
              assertEquals("4.5", getFieldValue(doc1Hit, "rating"));
              assertEquals("Alice Smith", getFieldValue(doc1Hit, "metadata_author"));

              // Verify doc2
              SearchResponse.Hit doc2Hit = hitsByKey.get("doc2");
              assertNotNull(doc2Hit);
              assertEquals("Cooking Pasta Perfectly", getFieldValue(doc2Hit, "title"));
              assertEquals("cooking", getFieldValue(doc2Hit, "category"));
              assertEquals("Chef Mario", getFieldValue(doc2Hit, "metadata_author"));

              // Verify doc3
              SearchResponse.Hit doc3Hit = hitsByKey.get("doc3");
              assertNotNull(doc3Hit);
              assertEquals("Travel Guide: Tokyo", getFieldValue(doc3Hit, "title"));
              assertEquals("travel", getFieldValue(doc3Hit, "category"));
            });

    LOGGER.info("End-to-end ingestion test passed!");
  }

  @Test
  public void testSearchByCategory() {
    LOGGER.info("Testing search by category...");

    // Setup index and schema
    setupIndexAndSchema();

    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              SearchRequest searchRequest =
                  SearchRequest.newBuilder()
                      .setIndexName(INDEX_NAME)
                      .setStartHit(0)
                      .setTopHits(10)
                      .setQuery(
                          Query.newBuilder()
                              .setTermQuery(
                                  TermQuery.newBuilder()
                                      .setField("category")
                                      .setTextValue("technology")
                                      .build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);

              assertEquals(1, response.getTotalHits().getValue());
              assertEquals("doc1", getFieldValue(response.getHits(0), "id"));
              assertEquals("Machine Learning Basics", getFieldValue(response.getHits(0), "title"));
            });
  }

  @Test
  public void testFullTextSearch() {
    LOGGER.info("Testing full-text search...");

    // Setup index and schema
    setupIndexAndSchema();

    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              SearchRequest searchRequest =
                  SearchRequest.newBuilder()
                      .setIndexName(INDEX_NAME)
                      .setStartHit(0)
                      .setTopHits(10)
                      .setQuery(
                          Query.newBuilder()
                              .setMatchQuery(
                                  MatchQuery.newBuilder()
                                      .setField("content")
                                      .setQuery("pasta cooking")
                                      .build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);

              assertTrue(response.getTotalHits().getValue() > 0);
              // Should find the cooking document
              boolean foundCookingDoc =
                  response.getHitsList().stream()
                      .anyMatch(hit -> "doc2".equals(getFieldValue(hit, "id")));
              assertTrue(foundCookingDoc);
            });
  }

  private String getFieldValue(SearchResponse.Hit hit, String fieldName) {
    SearchResponse.Hit.FieldValue fieldValue = hit.getFieldsMap().get(fieldName).getFieldValue(0);
    if (fieldValue.hasDoubleValue()) {
      return String.valueOf(fieldValue.getDoubleValue());
    } else {
      return fieldValue.getTextValue();
    }
  }

  @AfterClass
  public static void cleanup() {
    LOGGER.info("Cleaning up test resources...");

    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }

    if (kafka != null) {
      kafka.stop();
    }

    if (network != null) {
      network.close();
    }

    LOGGER.info("Cleanup complete");
  }
}
