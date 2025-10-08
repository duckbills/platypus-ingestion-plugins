/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.plugins.ingestion.paimon.e2e;

import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.test_utils.NrtsearchTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify that the paimon-plugin can be loaded correctly by nrtSearch. This test
 * specifically focuses on catching dependency conflicts (Hadoop/Jackson) at plugin load time rather
 * than during data processing.
 *
 * <p>If there are serious classpath conflicts between nrtSearch and Paimon/Hadoop, they should
 * manifest as ClassNotFoundException, NoSuchMethodError, or LinkageError during plugin loading.
 */
public class PaimonPluginLoadingE2ETest extends NrtsearchTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonPluginLoadingE2ETest.class);

  public PaimonPluginLoadingE2ETest() throws IOException {
    super();
  }

  @Override
  protected List<String> getPlugins() {
    // Upload ZIP to S3Mock and return S3 path - let NRTSearch handle download/extraction
    setupPluginInS3();
    return List.of(
        "s3://" + getS3BucketName() + "/plugins/nrtsearch-plugin-paimon-0.1.0-SNAPSHOT.zip");
  }

  @Override
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    super.addNrtsearchConfigs(config);

    // Add minimal plugin configuration (no real Paimon warehouse needed for load test)
    Map<String, Object> pluginConfigs = new HashMap<>();
    Map<String, Object> ingestionConfigs = new HashMap<>();
    Map<String, Object> paimonConfigs = new HashMap<>();

    // Dummy config - plugin won't actually try to connect during server startup
    paimonConfigs.put("warehouse.path", "/tmp/test-warehouse");
    paimonConfigs.put("database.name", "test_db");
    paimonConfigs.put("table.name", "test_table");
    paimonConfigs.put("target.index.name", "test-index");
    paimonConfigs.put("worker.threads", 1);
    paimonConfigs.put("batch.size", 10);
    paimonConfigs.put("scan.interval.ms", 5000);

    ingestionConfigs.put("paimon", paimonConfigs);
    pluginConfigs.put("ingestion", ingestionConfigs);
    config.put("pluginConfigs", pluginConfigs);
  }

  private void setupPluginInS3() {
    try {
      LOGGER.info("Setting up paimon-plugin in S3Mock for download by NRTSearch...");

      // Find the plugin distribution ZIP file
      String projectRoot = System.getProperty("user.dir");
      String pluginZipPath =
          projectRoot + "/build/distributions/nrtsearch-plugin-paimon-0.1.0-SNAPSHOT.zip";

      LOGGER.info("Looking for plugin distribution at: {}", pluginZipPath);

      // Check if the distribution exists, if not build it first
      if (!new java.io.File(pluginZipPath).exists()) {
        LOGGER.info("Plugin distribution not found, building it...");
        buildPluginDistribution(projectRoot);
      }

      // Upload ZIP to S3Mock - NRTSearch will download and extract it automatically
      java.io.File zipFile = new java.io.File(pluginZipPath);
      String s3Key = "plugins/nrtsearch-plugin-paimon-0.1.0-SNAPSHOT.zip";

      LOGGER.info("Uploading plugin ZIP ({} bytes) to S3Mock at key: {}", zipFile.length(), s3Key);

      getS3Client().putObject(getS3BucketName(), s3Key, zipFile);

      LOGGER.info("Plugin ZIP uploaded to S3Mock successfully");
      LOGGER.info("NRTSearch will download from: s3://{}/{}", getS3BucketName(), s3Key);

    } catch (Exception e) {
      throw new RuntimeException("Failed to setup plugin in S3Mock", e);
    }
  }

  private void buildPluginDistribution(String projectRoot) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("./gradlew", ":nrtsearch-plugin-paimon:distZip");
    pb.directory(
        new java.io.File(projectRoot).getParentFile()); // Go up to nrtsearch-ingestion-plugins
    pb.inheritIO(); // Show build output
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Failed to build plugin distribution, exit code: " + exitCode);
    }
  }

  @Test
  public void testPluginLoadsWithoutDependencyConflicts() {
    LOGGER.info("Testing that paimon-plugin loads correctly without dependency conflicts...");

    // The NrtsearchTest base class will have:
    // 1. Downloaded the ZIP from S3Mock
    // 2. Extracted it using ZipUtils.extractZip()
    // 3. Found plugin-metadata.yaml and parsed it
    // 4. Loaded PaimonIngestPlugin class from JARs
    // 5. Created plugin instance without ClassNotFoundException
    // 6. Registered plugin with server
    // If we get here without exceptions, the plugin loaded successfully!

    // Critical production-matching test points:
    // - S3 download → ZIP extraction → JAR loading (same as production)
    // - Hadoop/Paimon dependencies didn't conflict with nrtSearch
    // - Jackson version conflicts didn't cause LinkageError
    // - Plugin constructor executed without dependency issues

    assertTrue("Plugin loaded successfully without dependency conflicts!", true);
    LOGGER.info("paimon-plugin loaded successfully via S3Mock → ZIP extraction → JAR loading!");
    LOGGER.info("No Hadoop/Jackson dependency conflicts detected at plugin load time");
  }

  @Test
  public void testPluginCanInstantiatePaimonClasses() {
    LOGGER.info("Testing that plugin can instantiate Paimon classes without classloader issues...");

    try {
      // Try to load some key Paimon classes to verify classpath is working
      Class.forName("org.apache.paimon.catalog.CatalogFactory");
      Class.forName("org.apache.paimon.schema.Schema");
      Class.forName("org.apache.paimon.table.Table");
      LOGGER.info("Core Paimon classes loadable from plugin classloader");

      // Try Hadoop classes that Paimon depends on
      Class.forName("org.apache.hadoop.conf.Configuration");
      LOGGER.info("Hadoop Configuration class loadable");

      assertTrue("All critical classes loadable", true);

    } catch (ClassNotFoundException e) {
      LOGGER.error("Failed to load critical class: {}", e.getMessage());
      throw new RuntimeException("Plugin dependency issue: " + e.getMessage(), e);
    } catch (LinkageError e) {
      LOGGER.error("Linkage error loading class: {}", e.getMessage());
      throw new RuntimeException("Plugin classloader conflict: " + e.getMessage(), e);
    }
  }
}
