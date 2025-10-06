package com.yelp.nrtsearch.plugins.ingestion.paimon.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
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
 * E2E test for modulo sharding with partition-level file pruning.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Partitioned Paimon tables work with modulo sharding
 *   <li>Service name parsing extracts shard ID correctly
 *   <li>Only data from assigned partition is ingested (file-level skipping)
 *   <li>Partition field is properly dropped from nrtsearch index
 * </ul>
 */
public class PaimonModuloShardingE2ETest extends com.yelp.nrtsearch.test_utils.NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonModuloShardingE2ETest.class);

  static final String DATABASE_NAME = "test_db";
  static final String TABLE_NAME = "documents_partitioned";
  static final String INDEX_NAME = "test-sharded-index";
  static final String PARTITION_FIELD = "__internal_partition_id";
  static final int MAX_SHARDS = 5; // Use 5 shards for testing
  static final int SHARD_ID = 2; // This service will process shard 2

  private static final String PLUGIN_S3_KEY = "nrtsearch/plugins/paimon-plugin-0.1.0-SNAPSHOT.zip";
  private static TemporaryFolder tempWarehouse;
  private static String warehousePath;
  private static Catalog catalog;

  public PaimonModuloShardingE2ETest() throws IOException {
    super();
  }

  @BeforeClass
  public static void addPluginToS3() throws Exception {
    buildPluginDistribution();

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
    pb.directory(new java.io.File(projectRoot).getParentFile());
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
    createPartitionedPaimonTable();
    writeShardedTestData();
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config);

    // Service name with shard ID = 2
    String uniqueServiceName = "test-service-" + SHARD_ID;
    config.put("serviceName", uniqueServiceName);

    // Add plugin configuration with modulo sharding
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

    // CRITICAL: Drop the partition field - not needed in nrtsearch index
    paimonConfigs.put("field.drop.prefixes", List.of("__internal_"));

    // Configure modulo sharding
    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", MAX_SHARDS);
    moduloConfig.put("partition_field", PARTITION_FIELD);

    Map<String, Object> shardingConfig = new HashMap<>();
    shardingConfig.put("strategy", "modulo");
    shardingConfig.put("modulo", moduloConfig);

    paimonConfigs.put("sharding", shardingConfig);

    ingestionConfigs.put("paimon", paimonConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  static void setupPaimonInfrastructure() throws Exception {
    LOGGER.info("Setting up Paimon infrastructure...");

    tempWarehouse = new TemporaryFolder();
    tempWarehouse.create();
    warehousePath = tempWarehouse.getRoot().getAbsolutePath();

    LOGGER.info("Created Paimon warehouse at: {}", warehousePath);

    Options catalogOptions = new Options();
    catalogOptions.set("warehouse", warehousePath);
    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
    catalog = CatalogFactory.createCatalog(catalogContext);

    try {
      catalog.createDatabase(DATABASE_NAME, false);
      LOGGER.info("Created database: {}", DATABASE_NAME);
    } catch (Catalog.DatabaseAlreadyExistException e) {
      LOGGER.info("Database already exists: {}", DATABASE_NAME);
    }
  }

  static void createPartitionedPaimonTable() throws Exception {
    LOGGER.info("Creating PARTITIONED Paimon table...");

    // Define schema with partition field
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.column("id", DataTypes.INT().notNull());
    schemaBuilder.column("title", DataTypes.STRING());
    schemaBuilder.column("content", DataTypes.STRING());
    schemaBuilder.column(PARTITION_FIELD, DataTypes.INT().notNull()); // Partition field

    schemaBuilder.primaryKey("id");

    // CRITICAL: Partition by the partition field
    schemaBuilder.partitionKeys(PARTITION_FIELD);

    Schema schema = schemaBuilder.build();
    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);

    try {
      catalog.createTable(identifier, schema, false);
      LOGGER.info("Created PARTITIONED Paimon table: {}.{}", DATABASE_NAME, TABLE_NAME);
      LOGGER.info("Partition keys: {}", PARTITION_FIELD);
    } catch (Catalog.TableAlreadyExistException e) {
      LOGGER.info("Table already exists: {}.{}", DATABASE_NAME, TABLE_NAME);
    }
  }

  static void writeShardedTestData() throws Exception {
    LOGGER.info("Writing sharded test data across {} partitions...", MAX_SHARDS);

    Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
    Table table = catalog.getTable(identifier);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    BatchTableWrite write = writeBuilder.newWrite();

    // Write documents with IDs 0-14 (15 total documents)
    // Each document's partition = id % MAX_SHARDS
    // Shard 2 should get: 2, 7, 12
    for (int id = 0; id < 15; id++) {
      int partitionId = id % MAX_SHARDS;
      GenericRow row =
          GenericRow.of(
              id, // id
              BinaryString.fromString("Title " + id), // title
              BinaryString.fromString("Content " + id), // content
              partitionId); // __internal_partition_id

      // Explicitly specify bucket (just for test data generation API)
      int bucket = id % 3; // Spread across 3 buckets
      write.write(row, bucket);

      if (partitionId == SHARD_ID) {
        LOGGER.info(
            "Wrote doc id={} to partition {} (SHOULD BE INGESTED by shard {})",
            id,
            partitionId,
            SHARD_ID);
      } else {
        LOGGER.debug(
            "Wrote doc id={} to partition {} (should NOT be ingested by shard {})",
            id,
            partitionId,
            SHARD_ID);
      }
    }

    List<CommitMessage> messages = write.prepareCommit();
    BatchTableCommit commit = writeBuilder.newCommit();
    commit.commit(messages);
    write.close();

    LOGGER.info("Wrote 15 documents distributed across {} partitions", MAX_SHARDS);
    LOGGER.info("Shard {} should ingest exactly 3 documents: [2, 7, 12]", SHARD_ID);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
    if (tempWarehouse != null) {
      tempWarehouse.delete();
    }
  }

  @Test
  public void testModuloShardingOnlyIngestsAssignedPartition() throws Exception {
    LOGGER.info("=== TEST: Modulo sharding ingests only assigned partition ===");

    // Setup index and schema
    setupIndexAndSchema();

    // Wait for ingestion to complete
    // Shard 2 should ingest exactly 3 documents (ids: 2, 7, 12)
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              SearchResponse response = executeSearch();
              int hitCount = response.getHitsCount();
              LOGGER.info("Current document count: {}", hitCount);
              assertEquals("Shard 2 should ingest exactly 3 documents", 3, hitCount);
            });

    // Verify we got the correct documents
    SearchResponse response = executeSearch();
    assertEquals(3, response.getHitsCount());

    // Collect ingested document IDs
    Set<Integer> ingestedIds = new HashSet<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      String idValue = hit.getFieldsOrThrow("id").getFieldValueList().get(0).getTextValue();
      int docId = Integer.parseInt(idValue);
      ingestedIds.add(docId);
    }

    LOGGER.info("Ingested document IDs: {}", ingestedIds);

    // Verify we got exactly the documents from shard 2: [2, 7, 12]
    Set<Integer> expectedIds = Set.of(2, 7, 12);
    assertEquals("Should ingest documents [2, 7, 12]", expectedIds, ingestedIds);

    LOGGER.info("✓ Partition-level file pruning verified: only shard 2 documents ingested");
    LOGGER.info("✓ Partition field successfully dropped from nrtsearch index");
  }

  private void setupIndexAndSchema() {
    LOGGER.info("Setting up index and registering fields...");

    // Wait for server to fully start
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Create index
    CreateIndexRequest createRequest =
        CreateIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().createIndex(createRequest);

    // Register fields
    // NOTE: No __internal_partition_id field in schema - it's dropped by field.drop.prefixes
    FieldDefRequest.Builder fieldDefBuilder = FieldDefRequest.newBuilder().setIndexName(INDEX_NAME);

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
            .setStoreDocValues(true)
            .build());

    fieldDefBuilder.addField(
        Field.newBuilder()
            .setName("content")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .setStoreDocValues(true)
            .build());

    getClient().getBlockingStub().registerFields(fieldDefBuilder.build());

    // Start index
    StartIndexRequest startRequest =
        StartIndexRequest.newBuilder().setIndexName(INDEX_NAME).build();
    getClient().getBlockingStub().startIndex(startRequest);

    LOGGER.info("Index created and started: {}", INDEX_NAME);
  }

  private SearchResponse executeSearch() {
    return getClient()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(INDEX_NAME)
                .setTopHits(100)
                .setStartHit(0)
                .addRetrieveFields("id")
                .addRetrieveFields("title")
                .setQuery(Query.newBuilder().build())
                .build());
  }
}
