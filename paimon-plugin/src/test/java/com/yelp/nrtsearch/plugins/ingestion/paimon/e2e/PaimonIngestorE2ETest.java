package com.yelp.nrtsearch.plugins.ingestion.paimon.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive E2E test for Paimon ingestion plugin.
 *
 * <p>Test Flow: 1. Create temporary Paimon warehouse and tables using Java API 2. Start real
 * nrtSearch server with plugin loading 3. Create index and register schema matching Paimon table 4.
 * Write test data to Paimon table using batch writes 5. Wait for ingestion to index documents 6.
 * Query nrtSearch to verify data
 */
public class PaimonIngestorE2ETest extends com.yelp.nrtsearch.test_utils.NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonIngestorE2ETest.class);

  static final String DATABASE_NAME = "test_db";
  static final String TABLE_NAME = "documents";
  static final String INDEX_NAME = "test-index";
  static final String S3_BUCKET_NAME = "test-bucket";

  private static final String PLUGIN_S3_KEY = "nrtsearch/plugins/paimon-plugin-0.1.0-SNAPSHOT.zip";
  private static TemporaryFolder tempWarehouse;
  private static String warehousePath;
  private static Catalog catalog;

  public PaimonIngestorE2ETest() throws IOException {
    super();
  }

  @BeforeClass
  public static void addPluginToS3() throws Exception {
    // Build the plugin distribution first
    buildPluginDistribution();

    // Upload the plugin zip to S3
    String projectRoot = System.getProperty("user.dir");
    String pluginZipPath = projectRoot + "/build/distributions/paimon-plugin-0.1.0-SNAPSHOT.zip";

    getS3Client().putObject(getS3BucketName(), PLUGIN_S3_KEY, new java.io.File(pluginZipPath));
  }

  @Override
  protected List<String> getPlugins() {
    String path = String.format("s3://%s/%s", getS3BucketName(), PLUGIN_S3_KEY);
    return List.of(path);
  }

  private static void buildPluginDistribution() throws Exception {
    String projectRoot = System.getProperty("user.dir");
    ProcessBuilder pb = new ProcessBuilder("./gradlew", ":paimon-plugin:distZip");
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
    setupPaimonInfrastructure();
    createPaimonTable();
    writeTestDataToPaimon();
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config); // Get the defaults including plugin setup

    // Add plugin configuration for Paimon
    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> paimonConfigs = new HashMap<>();

    paimonConfigs.put("warehouse.path", warehousePath);
    paimonConfigs.put("table.path", DATABASE_NAME + "." + TABLE_NAME);
    paimonConfigs.put("target.index.name", INDEX_NAME);
    paimonConfigs.put("worker.threads", 2);
    paimonConfigs.put("batch.size", 10);
    paimonConfigs.put("scan.interval.ms", 1000);

    ingestionConfigs.put("paimon", paimonConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  static void setupPaimonInfrastructure() throws Exception {
    LOGGER.info("Setting up Paimon infrastructure...");

    // Create temporary warehouse directory
    tempWarehouse = new TemporaryFolder();
    tempWarehouse.create();
    warehousePath = tempWarehouse.getRoot().getAbsolutePath();

    LOGGER.info("Created Paimon warehouse at: {}", warehousePath);

    // Create filesystem catalog
    Options catalogOptions = new Options();
    catalogOptions.set("warehouse", warehousePath);
    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
    catalog = CatalogFactory.createCatalog(catalogContext);

    // Create database
    try {
      catalog.createDatabase(DATABASE_NAME, false);
      LOGGER.info("Created database: {}", DATABASE_NAME);
    } catch (Catalog.DatabaseAlreadyExistException e) {
      LOGGER.info("Database already exists: {}", DATABASE_NAME);
    }
  }

  static void createPaimonTable() throws Exception {
    LOGGER.info("Creating Paimon table...");

    // Define schema matching test documents
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.column("id", DataTypes.STRING().notNull());
    schemaBuilder.column("title", DataTypes.STRING());
    schemaBuilder.column("content", DataTypes.STRING());
    schemaBuilder.column("category", DataTypes.STRING());
    schemaBuilder.column("rating", DataTypes.DOUBLE());
    schemaBuilder.column("tags", DataTypes.ARRAY(DataTypes.STRING()));
    schemaBuilder.column("metadata_author", DataTypes.STRING());
    schemaBuilder.column("metadata_publishDate", DataTypes.STRING());

    // Set primary key for efficient lookups
    schemaBuilder.primaryKey("id");

    Schema schema = schemaBuilder.build();
    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);

    try {
      catalog.createTable(identifier, schema, false);
      LOGGER.info("Created Paimon table: {}.{}", DATABASE_NAME, TABLE_NAME);
    } catch (Catalog.TableAlreadyExistException e) {
      LOGGER.info("Table already exists: {}.{}", DATABASE_NAME, TABLE_NAME);
    }
  }

  static void writeTestDataToPaimon() throws Exception {
    LOGGER.info("Writing test data to Paimon table...");

    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);

    // Create batch writer
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    BatchTableWrite write = writeBuilder.newWrite();

    // Create test documents
    List<GenericRow> testDocs = createTestDocuments();

    // Write documents with bucket determined by primary key hash (ensures same keys go to same
    // bucket)
    final int numBuckets = 3;
    for (GenericRow doc : testDocs) {
      String id = doc.getField(0).toString(); // id is the primary key
      int bucket = Math.abs(id.hashCode()) % numBuckets;
      write.write(doc, bucket);
      LOGGER.debug("Wrote document: {}", doc.getField(0)); // id field
    }

    // Prepare commit
    List<CommitMessage> messages = write.prepareCommit();

    // Commit the batch
    BatchTableCommit commit = writeBuilder.newCommit();
    commit.commit(messages);

    LOGGER.info("Successfully wrote {} documents to Paimon table", testDocs.size());
  }

  static List<GenericRow> createTestDocuments() {
    List<GenericRow> docs = new ArrayList<>();

    // Document 1
    GenericRow doc1 =
        GenericRow.of(
            BinaryString.fromString("doc1"),
            BinaryString.fromString("Machine Learning Basics"),
            BinaryString.fromString("Introduction to neural networks and deep learning concepts"),
            BinaryString.fromString("technology"),
            4.5,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {
                  BinaryString.fromString("ml"),
                  BinaryString.fromString("ai"),
                  BinaryString.fromString("tutorial")
                }),
            BinaryString.fromString("Alice Smith"),
            BinaryString.fromString("2024-01-15"));
    docs.add(doc1);

    // Document 2
    GenericRow doc2 =
        GenericRow.of(
            BinaryString.fromString("doc2"),
            BinaryString.fromString("Cooking Pasta Perfectly"),
            BinaryString.fromString("Tips and tricks for making restaurant-quality pasta at home"),
            BinaryString.fromString("cooking"),
            4.8,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {
                  BinaryString.fromString("food"),
                  BinaryString.fromString("recipe"),
                  BinaryString.fromString("italian")
                }),
            BinaryString.fromString("Chef Mario"),
            BinaryString.fromString("2024-01-20"));
    docs.add(doc2);

    // Document 3
    GenericRow doc3 =
        GenericRow.of(
            BinaryString.fromString("doc3"),
            BinaryString.fromString("Travel Guide: Tokyo"),
            BinaryString.fromString("Best places to visit and authentic food experiences in Tokyo"),
            BinaryString.fromString("travel"),
            4.2,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {
                  BinaryString.fromString("travel"),
                  BinaryString.fromString("japan"),
                  BinaryString.fromString("guide")
                }),
            BinaryString.fromString("Travel Blogger"),
            BinaryString.fromString("2024-01-25"));
    docs.add(doc3);

    return docs;
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

    // Register fields matching Paimon schema
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

    // Metadata fields (flattened with underscores)
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

  @Test
  public void testEndToEndPaimonIngestion() {
    LOGGER.info("Testing end-to-end Paimon ingestion...");

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

    LOGGER.info("✅ End-to-end ingestion test passed!");
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

    try {
      if (catalog != null) {
        catalog.close();
      }
    } catch (Exception e) {
      LOGGER.warn("Error closing catalog", e);
    }

    try {
      if (tempWarehouse != null) {
        tempWarehouse.delete();
      }
    } catch (Exception e) {
      LOGGER.warn("Error cleaning up temporary warehouse", e);
    }

    LOGGER.info("Cleanup complete");
  }
}
