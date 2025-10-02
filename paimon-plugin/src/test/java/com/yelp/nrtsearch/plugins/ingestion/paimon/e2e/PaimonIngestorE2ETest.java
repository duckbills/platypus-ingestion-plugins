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

    // Use unique serviceName per test run to avoid consumer ID conflicts
    String uniqueServiceName =
        "test-service-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    config.put("serviceName", uniqueServiceName);

    // Add plugin configuration for Paimon
    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> paimonConfigs = new HashMap<>();

    paimonConfigs.put("warehouse.path", warehousePath);
    paimonConfigs.put("database.name", DATABASE_NAME);
    paimonConfigs.put("table.name", TABLE_NAME);
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
    LOGGER.info("Writing minimal dataset for split ordering test...");

    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

    // SNAPSHOT 1: Initial writes to multiple buckets with some sharing same bucket
    LOGGER.info("Writing snapshot 1: Initial documents");
    BatchTableWrite write1 = writeBuilder.newWrite();

    GenericRow user123v1 =
        createVersionedDoc("user123", "v1", "Initial data for user123", "initial");
    GenericRow user456v1 =
        createVersionedDoc("user456", "v1", "Initial data for user456", "initial");
    GenericRow user789v1 =
        createVersionedDoc("user789", "v1", "Initial data for user789", "initial");

    // Force specific bucket assignment to ensure we get shared buckets
    write1.write(user123v1, 0); // bucket 0
    write1.write(user456v1, 1); // bucket 1
    write1.write(user789v1, 0); // bucket 0 (same as user123 - creates multiple docs in same bucket)

    List<CommitMessage> messages1 = write1.prepareCommit();
    BatchTableCommit commit1 = writeBuilder.newCommit();
    commit1.commit(messages1);
    write1.close();

    LOGGER.info("Snapshot 1: user123+user789 in bucket0, user456 in bucket1");
    Thread.sleep(1000);

    // SNAPSHOT 2: Updates to existing users + new user in same bucket
    LOGGER.info("Writing snapshot 2: Updates + new user");
    BatchTableWrite write2 = writeBuilder.newWrite();

    GenericRow user123v2 =
        createVersionedDoc("user123", "v2", "Updated data for user123", "updated");
    GenericRow user456v2 =
        createVersionedDoc("user456", "v2", "Updated data for user456", "updated");
    GenericRow user999v1 = createVersionedDoc("user999", "v1", "New user999 initial data", "new");

    // Same bucket assignments as their previous versions + new user in bucket 0
    write2.write(user123v2, 0); // bucket 0 (UPDATE)
    write2.write(user456v2, 1); // bucket 1 (UPDATE)
    write2.write(user999v1, 0); // bucket 0 (NEW - creates multiple files in bucket 0)

    List<CommitMessage> messages2 = write2.prepareCommit();
    BatchTableCommit commit2 = writeBuilder.newCommit();
    commit2.commit(messages2);
    write2.close();

    LOGGER.info("Snapshot 2: user123+user999 in bucket0, user456 in bucket1");
    Thread.sleep(1000);

    // SNAPSHOT 3: Final update to user123 only
    LOGGER.info("Writing snapshot 3: Final user123 update");
    BatchTableWrite write3 = writeBuilder.newWrite();

    GenericRow user123v3 =
        createVersionedDoc("user123", "v3", "Final version for user123", "final");

    write3.write(user123v3, 0); // bucket 0 (FINAL UPDATE)

    List<CommitMessage> messages3 = write3.prepareCommit();
    BatchTableCommit commit3 = writeBuilder.newCommit();
    commit3.commit(messages3);
    write3.close();

    LOGGER.info("Successfully wrote 3 snapshots with realistic split distribution");
    LOGGER.info("Expected splits: s1/b0, s1/b1, s2/b0, s2/b1, s3/b0");
    LOGGER.info("Test case: user123 should have final version v3 if ordering works correctly");
  }

  /** Helper method to create versioned documents for split ordering test */
  private static GenericRow createVersionedDoc(
      String userId, String version, String content, String category) {
    return GenericRow.of(
        BinaryString.fromString(userId),
        BinaryString.fromString("User " + userId + " Document " + version),
        BinaryString.fromString(content),
        BinaryString.fromString(category),
        Double.parseDouble(version.substring(1)) + 3.0, // v1=4.0, v2=5.0, v3=6.0
        new org.apache.paimon.data.GenericArray(
            new BinaryString[] {
              BinaryString.fromString("test"), BinaryString.fromString("version" + version)
            }),
        BinaryString.fromString("Test Author"),
        BinaryString.fromString("2024-01-0" + version.substring(1))); // v1->01, v2->02, v3->03
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
            .setType(FieldType._ID)
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

              // Verify we got at least the 4 core documents (user123, user456, user789, user999)
              // May have more if other tests like testDeleteSupport() have run
              assertTrue("Expected at least 4 documents", response.getTotalHits().getValue() >= 4);

              // Verify document content
              Map<String, SearchResponse.Hit> hitsByKey = new HashMap<>();
              for (SearchResponse.Hit hit : response.getHitsList()) {
                String id = getFieldValue(hit, "id");
                hitsByKey.put(id, hit);
              }

              // Verify user123 (final version v3)
              SearchResponse.Hit user123Hit = hitsByKey.get("user123");
              assertNotNull(user123Hit);
              assertEquals("User user123 Document v3", getFieldValue(user123Hit, "title"));
              assertEquals("final", getFieldValue(user123Hit, "category"));
              assertEquals("6.0", getFieldValue(user123Hit, "rating"));
              assertEquals("Test Author", getFieldValue(user123Hit, "metadata_author"));

              // Verify user456 (final version v2)
              SearchResponse.Hit user456Hit = hitsByKey.get("user456");
              assertNotNull(user456Hit);
              assertEquals("User user456 Document v2", getFieldValue(user456Hit, "title"));
              assertEquals("updated", getFieldValue(user456Hit, "category"));
              assertEquals("Test Author", getFieldValue(user456Hit, "metadata_author"));

              // Verify user789 (version v1 only)
              SearchResponse.Hit user789Hit = hitsByKey.get("user789");
              assertNotNull(user789Hit);
              assertEquals("User user789 Document v1", getFieldValue(user789Hit, "title"));
              assertEquals("initial", getFieldValue(user789Hit, "category"));

              // Verify user999 (version v1 only)
              SearchResponse.Hit user999Hit = hitsByKey.get("user999");
              assertNotNull(user999Hit);
              assertEquals("User user999 Document v1", getFieldValue(user999Hit, "title"));
              assertEquals("new", getFieldValue(user999Hit, "category"));
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
                                      .setTextValue("final")
                                      .build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);

              assertEquals(1, response.getTotalHits().getValue());
              assertEquals("user123", getFieldValue(response.getHits(0), "id"));
              assertEquals("User user123 Document v3", getFieldValue(response.getHits(0), "title"));
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
                                      .setQuery("user123 Final")
                                      .build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);

              assertTrue(response.getTotalHits().getValue() > 0);
              // Should find the user123 document
              boolean foundUser123Doc =
                  response.getHitsList().stream()
                      .anyMatch(hit -> "user123".equals(getFieldValue(hit, "id")));
              assertTrue(foundUser123Doc);
            });
  }

  @Test
  public void testMultiSnapshotOrdering() throws Exception {
    LOGGER.info("Testing multi-snapshot split ordering...");

    // Setup index and schema first
    setupIndexAndSchema();

    // The writeTestDataToPaimon() method already created multi-snapshot test data

    // Wait for all snapshots to be processed
    await()
        .atMost(45, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(2))
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
                      .addRetrieveFields("content")
                      .addRetrieveFields("category")
                      .addRetrieveFields("rating")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);
              LOGGER.info(
                  "Multi-snapshot test: found {} documents", response.getTotalHits().getValue());

              // Should have at least 5 documents (user123, user456, user789, user999 + any others)
              assertTrue(
                  "Should have multiple documents from snapshots",
                  response.getTotalHits().getValue() >= 4);

              // Find user123 who had multiple versions across snapshots
              SearchResponse.Hit user123Doc = null;
              for (SearchResponse.Hit hit : response.getHitsList()) {
                if ("user123".equals(getFieldValue(hit, "id"))) {
                  user123Doc = hit;
                  break;
                }
              }

              // Verify user123 has the final version (v3) if split ordering worked correctly
              assertNotNull("Should find user123 document", user123Doc);
              assertEquals(
                  "user123 should have final version content",
                  "Final version for user123",
                  getFieldValue(user123Doc, "content"));
              assertEquals(
                  "user123 should have final title",
                  "User user123 Document v3",
                  getFieldValue(user123Doc, "title"));
              assertEquals(
                  "user123 should have final category",
                  "final",
                  getFieldValue(user123Doc, "category"));
              assertEquals(
                  "user123 should have final rating", "6.0", getFieldValue(user123Doc, "rating"));

              LOGGER.info("Multi-snapshot ordering verified: document has latest version");
            });
  }

  @Test
  public void testDeleteSupport() throws Exception {
    LOGGER.info("Testing DELETE support with INSERT→DELETE→INSERT sequence...");

    // Setup index and schema first
    setupIndexAndSchema();

    // Write test data with explicit DELETE operation
    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

    // Snapshot 1: Insert test_delete_user
    BatchTableWrite write1 = writeBuilder.newWrite();
    GenericRow insertV1 =
        GenericRow.ofKind(
            org.apache.paimon.types.RowKind.INSERT,
            BinaryString.fromString("test_delete_user"),
            BinaryString.fromString("First Version"),
            BinaryString.fromString("This should be deleted"),
            BinaryString.fromString("delete-test-v1"),
            1.0,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {BinaryString.fromString("delete-test")}),
            BinaryString.fromString("Test Author"),
            BinaryString.fromString("2024-01-01"));
    write1.write(insertV1, 0);
    List<CommitMessage> messages1 = write1.prepareCommit();
    writeBuilder.newCommit().commit(messages1);
    write1.close();
    Thread.sleep(1000);

    // Snapshot 2: Delete test_delete_user
    BatchTableWrite write2 = writeBuilder.newWrite();
    GenericRow delete =
        GenericRow.ofKind(
            org.apache.paimon.types.RowKind.DELETE,
            BinaryString.fromString("test_delete_user"),
            BinaryString.fromString("First Version"),
            BinaryString.fromString("This should be deleted"),
            BinaryString.fromString("delete-test-v1"),
            1.0,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {BinaryString.fromString("delete-test")}),
            BinaryString.fromString("Test Author"),
            BinaryString.fromString("2024-01-01"));
    write2.write(delete, 0);
    List<CommitMessage> messages2 = write2.prepareCommit();
    writeBuilder.newCommit().commit(messages2);
    write2.close();
    Thread.sleep(1000);

    // Snapshot 3: Insert test_delete_user again (different content)
    BatchTableWrite write3 = writeBuilder.newWrite();
    GenericRow insertV2 =
        GenericRow.ofKind(
            org.apache.paimon.types.RowKind.INSERT,
            BinaryString.fromString("test_delete_user"),
            BinaryString.fromString("Second Version"),
            BinaryString.fromString("This is the final version"),
            BinaryString.fromString("delete-test-final"),
            2.0,
            new org.apache.paimon.data.GenericArray(
                new BinaryString[] {BinaryString.fromString("delete-test")}),
            BinaryString.fromString("Test Author"),
            BinaryString.fromString("2024-01-02"));
    write3.write(insertV2, 0);
    List<CommitMessage> messages3 = write3.prepareCommit();
    writeBuilder.newCommit().commit(messages3);
    write3.close();

    LOGGER.info("Wrote 3 snapshots: INSERT → DELETE → INSERT for same key");

    // Wait for all snapshots to be processed and verify final state
    await()
        .atMost(45, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(2))
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
                                      .setTextValue("delete-test-final")
                                      .build())
                              .build())
                      .addRetrieveFields("id")
                      .addRetrieveFields("title")
                      .addRetrieveFields("content")
                      .addRetrieveFields("category")
                      .build();

              SearchResponse response = getClient().getBlockingStub().search(searchRequest);

              LOGGER.info("Found {} hits for test_delete_user", response.getTotalHits().getValue());

              // Should find exactly 1 document (the final INSERT after DELETE)
              assertEquals(
                  "Should have exactly one document after INSERT→DELETE→INSERT",
                  1,
                  response.getTotalHits().getValue());

              SearchResponse.Hit hit = response.getHits(0);
              assertEquals("test_delete_user", getFieldValue(hit, "id"));
              assertEquals(
                  "Second Version",
                  getFieldValue(hit, "title")); // This proves all 3 snapshots processed
              assertEquals("This is the final version", getFieldValue(hit, "content"));
              assertEquals("delete-test-final", getFieldValue(hit, "category"));

              LOGGER.info("DELETE support verified: final document has correct content");
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
