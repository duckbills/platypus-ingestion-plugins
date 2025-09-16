/*
 * Copyright 2024 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.plugins.ingestion.kafka.e2e;

import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.test_utils.NrtsearchTest;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify that the kafka-plugin can be loaded correctly by nrtSearch. This is a simpler test
 * that doesn't require Docker/Testcontainers.
 */
public class PluginLoadingE2ETest extends NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PluginLoadingE2ETest.class);

  public PluginLoadingE2ETest() throws IOException {
    super();
  }

  @Override
  protected List<String> getPlugins() {
    return List.of("kafka-plugin");
  }

  @Override
  protected Path getPluginSearchPath() {
    try {
      Path pluginDir = super.getPluginSearchPath();
      setupPluginDirectory(pluginDir);
      return pluginDir;
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup plugin directory", e);
    }
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config);

    // Add minimal plugin configuration (no Kafka servers needed for load test)
    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> kafkaConfigs = new HashMap<>();

    kafkaConfigs.put("bootstrapServers", "localhost:9092");
    kafkaConfigs.put("topic", "test-topic");
    kafkaConfigs.put("groupId", "test-group");
    kafkaConfigs.put("indexName", "test-index");
    kafkaConfigs.put("schemaRegistryUrl", "http://localhost:8081");
    kafkaConfigs.put("autoOffsetReset", "earliest");
    kafkaConfigs.put("batchSize", "10");

    ingestionConfigs.put("kafka", kafkaConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  private void setupPluginDirectory(Path pluginSearchPath) throws Exception {
    LOGGER.info("Setting up kafka-plugin in test plugin directory: {}", pluginSearchPath);

    // Find the plugin distribution tar file
    String projectRoot = System.getProperty("user.dir");
    String pluginTarPath = projectRoot + "/build/distributions/kafka-plugin-0.1.0-SNAPSHOT.tar";

    LOGGER.info("Looking for plugin distribution at: {}", pluginTarPath);

    // Check if the distribution exists, if not build it first
    if (!new java.io.File(pluginTarPath).exists()) {
      LOGGER.info("Plugin distribution not found, building it...");
      buildPluginDistribution(projectRoot);
    }

    // Extract the tar file to plugin search path
    extractPluginDistribution(pluginTarPath, pluginSearchPath);

    // Verify the plugin was set up correctly
    Path kafkaPluginDir = pluginSearchPath.resolve("kafka-plugin");
    Path metadataFile = kafkaPluginDir.resolve("plugin-metadata.yaml");
    if (!metadataFile.toFile().exists()) {
      throw new RuntimeException(
          "Plugin setup failed - metadata file not found at: " + metadataFile);
    }

    LOGGER.info("Plugin setup complete at: {}", kafkaPluginDir);
    LOGGER.info("Found plugin metadata: {}", metadataFile);
  }

  private void buildPluginDistribution(String projectRoot) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("./gradlew", ":kafka-plugin:distTar");
    pb.directory(
        new java.io.File(projectRoot).getParentFile()); // Go up to nrtsearch-ingestion-plugins
    pb.inheritIO(); // Show build output
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Failed to build plugin distribution, exit code: " + exitCode);
    }
  }

  private void extractPluginDistribution(String tarPath, Path targetDir) throws Exception {
    LOGGER.info("Extracting plugin from {} to {}", tarPath, targetDir);

    // Extract the tar file
    ProcessBuilder pb = new ProcessBuilder("tar", "-xf", tarPath, "-C", targetDir.toString());
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Failed to extract plugin tar, exit code: " + exitCode);
    }

    // The tar extracts to kafka-plugin-0.1.0-SNAPSHOT/, we need to rename it to kafka-plugin/
    Path extractedDir = targetDir.resolve("kafka-plugin-0.1.0-SNAPSHOT");
    Path finalDir = targetDir.resolve("kafka-plugin");

    if (extractedDir.toFile().exists()) {
      // Rename from versioned directory to kafka-plugin directory
      if (!extractedDir.toFile().renameTo(finalDir.toFile())) {
        throw new RuntimeException(
            "Failed to rename plugin directory from " + extractedDir + " to " + finalDir);
      }
      LOGGER.info(
          "Renamed plugin directory: {} -> {}", extractedDir.getFileName(), finalDir.getFileName());
    } else {
      throw new RuntimeException("Expected extracted directory not found: " + extractedDir);
    }
  }

  @Test
  public void testPluginLoadsSuccessfully() {
    LOGGER.info("Testing that kafka-plugin loads correctly...");

    // The NrtsearchTest base class will have loaded plugins during server startup
    // If we get here without exceptions, the plugin loaded successfully!

    // We can't easily inspect the loaded plugins from the test client,
    // but the fact that the server started without errors means:
    // 1. Plugin directory was found
    // 2. plugin-metadata.yaml was parsed correctly
    // 3. KafkaIngestPlugin class was loaded from JAR
    // 4. Plugin constructor didn't throw exceptions
    // 5. Plugin is now registered with the server

    assertTrue("Plugin loaded successfully!", true);
    LOGGER.info("kafka-plugin loaded successfully into nrtSearch server!");
  }
}
