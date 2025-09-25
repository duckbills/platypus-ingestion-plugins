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
package com.yelp.nrtsearch.plugins.ingestion.paimon;

import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.plugin.PluginLoader;

/**
 * A {@link FileIOLoader} to load S3FileIO for s3a:// scheme URLs. This extends Paimon's S3 support
 * to handle s3a scheme in addition to s3 scheme. Uses IAM role authentication, so no access keys
 * are required.
 */
public class S3ALoader implements FileIOLoader {

  private static final long serialVersionUID = 1L;

  private static final String S3_CLASSES_DIR = "paimon-plugin-s3";

  private static final String S3_CLASS = "org.apache.paimon.s3.S3FileIO";

  // Singleton lazy initialization
  private static PluginLoader loader;

  private static synchronized PluginLoader getLoader() {
    if (loader == null) {
      // Avoid NoClassDefFoundError without cause by exception
      loader = new PluginLoader(S3_CLASSES_DIR);
    }
    return loader;
  }

  @Override
  public String getScheme() {
    return "s3a"; // This is the key difference from S3Loader - handles s3a scheme
  }

  @Override
  public List<String[]> requiredOptions() {
    // No required options when using IAM roles for authentication
    return new ArrayList<>();
  }

  @Override
  public FileIO load(Path path) {
    return new S3APluginFileIO();
  }

  private static class S3APluginFileIO extends PluginFileIO {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isObjectStore() {
      return true;
    }

    @Override
    protected FileIO createFileIO(Path path) {
      // Load the same S3FileIO class that S3Loader uses
      FileIO fileIO = getLoader().newInstance(S3_CLASS);
      fileIO.configure(CatalogContext.create(options));
      return fileIO;
    }

    @Override
    protected ClassLoader pluginClassLoader() {
      return getLoader().submoduleClassLoader();
    }
  }
}
