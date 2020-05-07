/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.util;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Version information, retrieved from the pom (via a properties file reference).
 */
public final class REEFVersion {

  private static final Logger LOG = Logger.getLogger(REEFVersion.class.getName());

  private static final String FILENAME = "version.properties";
  private static final String VERSION_KEY = "version";
  private static final String VERSION_DEFAULT = "unknown";

  private final String version;

  @Inject
  public REEFVersion() {
    this.version = loadVersion();
  }

  /**
   * Logs the version of REEF into the log Level INFO.
   */
  public void logVersion() {
    this.logVersion(Level.INFO);
  }

  /**
   * Logs the version of REEF into the given logLevel.
   *
   * @param logLevel The level to use in the log.
   */
  public void logVersion(final Level logLevel) {
    LOG.log(logLevel, "REEF Version: {0}", this.version);
  }

  /**
   * @return the version string for REEF.
   */
  public String getVersion() {
    return version;
  }

  private static String loadVersion() {
    String version;
    try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(FILENAME)) {
      if (is == null) {
        throw new IOException(FILENAME + " not found");
      }
      final Properties properties = new Properties();
      properties.load(is);
      version = properties.getProperty(VERSION_KEY, VERSION_DEFAULT);
    } catch (final IOException e) {
      LOG.log(Level.WARNING, "Could not find REEF version");
      version = VERSION_DEFAULT;
    }
    return version;
  }
}
