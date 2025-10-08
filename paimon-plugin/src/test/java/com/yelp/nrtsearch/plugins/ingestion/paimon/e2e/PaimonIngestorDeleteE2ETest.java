package com.yelp.nrtsearch.plugins.ingestion.paimon.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

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

/** E2E test for DELETE support in Paimon plugin. */
public class PaimonIngestorDeleteE2ETest extends com.yelp.nrtsearch.test_utils.NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonIngestorDeleteE2ETest.class);

  static final String DATABASE_NAME = "delete_test_db";
  static final String TABLE_NAME = "delete_test_table";
  static final String INDEX_NAME = "delete-test-index";
  static final String S3_BUCKET_NAME = "test-bucket";

  private static final String PLUGIN_S3_KEY =
      "nrtsearch/plugins/nrtsearch-plugin-paimon-0.1.0-SNAPSHOT.zip";
  private static TemporaryFolder tempWarehouse;
  private static String warehousePath;
  private static Catalog catalog;

  public PaimonIngestorDeleteE2ETest() throws IOException {
    super();
  }

  @BeforeClass
  public static void addPluginToS3() throws Exception {
    buildPluginDistribution();

    String projectRoot = System.getProperty("user.dir");
    String pluginZipPath =
        projectRoot + "/build/distributions/nrtsearch-plugin-paimon-0.1.0-SNAPSHOT.zip";

    getS3Client().putObject(getS3BucketName(), PLUGIN_S3_KEY, new java.io.File(pluginZipPath));
  }

  @Override
  protected List<String> getPlugins() {
    String path = String.format("s3://%s/%s", getS3BucketName(), PLUGIN_S3_KEY);
    return List.of(path);
  }

  private static void buildPluginDistribution() throws Exception {
    String projectRoot = System.getProperty("user.dir");
    ProcessBuilder pb = new ProcessBuilder("./gradlew", ":nrtsearch-plugin-paimon:distZip");
    pb.directory(
        new java.io.File(projectRoot).getParentFile()); // Go up to nrtsearch-ingestion-plugins
    pb.inheritIO();
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
    writeInitialTestData();
  }

  static void setupPaimonInfrastructure() throws Exception {
    LOGGER.info("Setting up Paimon infrastructure for DELETE test...");

    tempWarehouse = new TemporaryFolder();
    tempWarehouse.create();
    warehousePath = tempWarehouse.getRoot().getAbsolutePath();

    Options catalogOptions = new Options();
    catalogOptions.set("warehouse", warehousePath);
    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
    catalog = CatalogFactory.createCatalog(catalogContext);

    catalog.createDatabase(DATABASE_NAME, false);
    LOGGER.info("Created DELETE test database at: {}", warehousePath);
  }

  static void createPaimonTable() throws Exception {
    LOGGER.info("Creating Paimon table for DELETE test...");

    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.column("id", DataTypes.STRING().notNull());
    schemaBuilder.column("title", DataTypes.STRING());
    schemaBuilder.column("category", DataTypes.STRING());
    schemaBuilder.primaryKey("id");

    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    catalog.createTable(identifier, schemaBuilder.build(), false);
    LOGGER.info("Created DELETE test table: {}.{}", DATABASE_NAME, TABLE_NAME);
  }

  static void writeInitialTestData() throws Exception {
    LOGGER.info("Writing initial test data for DELETE test...");

    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

    // Write initial INSERT
    BatchTableWrite write = writeBuilder.newWrite();
    GenericRow doc1 =
        GenericRow.of(
            BinaryString.fromString("doc1"), // id
            BinaryString.fromString("Test Document"), // title
            BinaryString.fromString("test-category")); // category
    write.write(doc1, 0);

    List<CommitMessage> messages = write.prepareCommit();
    BatchTableCommit commit = writeBuilder.newCommit();
    commit.commit(messages);
    write.close();

    LOGGER.info("Wrote initial INSERT for doc1");
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config);

    String uniqueServiceName =
        "delete-test-service-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    config.put("serviceName", uniqueServiceName);

    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> paimonConfigs = new HashMap<>();

    paimonConfigs.put("warehouse.path", warehousePath);
    paimonConfigs.put("database.name", DATABASE_NAME);
    paimonConfigs.put("table.name", TABLE_NAME);
    paimonConfigs.put("target.index.name", INDEX_NAME);
    paimonConfigs.put("worker.threads", 1);
    paimonConfigs.put("batch.size", 10);
    paimonConfigs.put("scan.interval.ms", 1000);

    ingestionConfigs.put("paimon", paimonConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  /** Test flow: INSERT → search (find 1) → DELETE → search (find 0) */
  @Test
  public void testDeleteSupport() throws Exception {
    LOGGER.info("Testing DELETE support: INSERT → find 1 → DELETE → find 0");

    setupIndexAndSchema();

    // STEP 1: Wait and verify 1 document exists (from @BeforeClass)
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
                      .addRetrieveFields("category")
                      .build();
              SearchResponse response = getClient().getBlockingStub().search(searchRequest);
              LOGGER.info("After INSERT: {} documents", response.getTotalHits().getValue());
              assertEquals(1, response.getTotalHits().getValue());
            });

    LOGGER.info("✓ Found 1 document after INSERT");

    // STEP 2: Write DELETE
    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

    BatchTableWrite write = writeBuilder.newWrite();
    GenericRow delete =
        GenericRow.ofKind(
            org.apache.paimon.types.RowKind.DELETE,
            BinaryString.fromString("doc1"), // id
            BinaryString.fromString("Test Document"), // title
            BinaryString.fromString("test-category")); // category
    write.write(delete, 0);

    List<CommitMessage> messages = write.prepareCommit();
    BatchTableCommit commit = writeBuilder.newCommit();
    commit.commit(messages);
    write.close();

    LOGGER.info("Wrote DELETE snapshot for doc1");

    // STEP 3: Wait and verify 0 documents exist
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
                      .build();
              SearchResponse response = getClient().getBlockingStub().search(searchRequest);
              LOGGER.info("After DELETE: {} documents", response.getTotalHits().getValue());
              assertEquals(0, response.getTotalHits().getValue());
            });

    LOGGER.info("✓ Found 0 documents after DELETE");
    LOGGER.info("DELETE support verified successfully!");
  }

  private void setupIndexAndSchema() {
    LOGGER.info("Setting up index and registering fields...");

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    CreateIndexRequest createRequest =
        CreateIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().createIndex(createRequest);

    FieldDefRequest.Builder fieldDefBuilder = FieldDefRequest.newBuilder().setIndexName(INDEX_NAME);

    // Register id field with FieldType._ID for DELETE operations
    fieldDefBuilder.addField(
        Field.newBuilder().setName("id").setType(FieldType._ID).setStoreDocValues(true).build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("title")
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

    getClient().getBlockingStub().registerFields(fieldDefBuilder.build());

    StartIndexRequest startRequest =
        StartIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().startIndex(startRequest);

    LOGGER.info("Index setup complete");
  }

  @AfterClass
  public static void cleanup() {
    LOGGER.info("Cleaning up DELETE test resources...");

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
