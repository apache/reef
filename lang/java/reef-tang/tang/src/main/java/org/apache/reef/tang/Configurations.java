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
package org.apache.reef.tang;

import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

/**
 * Helper class for Configurations.
 */
public final class Configurations {

  private static final AvroConfigurationSerializer SERIALIZER = new AvroConfigurationSerializer();

  /**
   * This is a utility class that isn't meant to be instantiated.
   */
  private Configurations() {
  }

  /**
   * Merge a set of Configurations.
   *
   * @param configurations the configuration to be merged
   * @return the merged configuration.
   * @throws org.apache.reef.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Configuration... configurations) {
    return Tang.Factory.getTang().newConfigurationBuilder(configurations).build();
  }

  /**
   * Merge a set of Configurations.
   *
   * @param configurations the configuration to be merged
   * @return the merged configuration.
   * @throws org.apache.reef.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Iterable<Configuration> configurations) {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Configuration configuration : configurations) {
      configurationBuilder.addConfiguration(configuration);
    }
    return configurationBuilder.build();
  }

  /**
   * Get the default configuration serializer.
   * Currently it is AvroConfigurationSerializer.
   * Use Tang.toString(...) to produce a human-readable string representation of the config.
   * @return configuration serializer object.
   */
  public static ConfigurationSerializer getDefaultSerializer() {
    return SERIALIZER;
  }

  /**
   * Return human-readable representation of the configuration.
   * @param config input configuration.
   * @return a string that contains human-readable representation of the input configuration.
   */
  public static String toString(final Configuration config) {
    return SERIALIZER.toString(config);
  }

  /**
   * Return human-readable representation of the configuration.
   * If prettyPrint is true, try to produce a nicer text layout, if possible.
   * @param config input configuration.
   * @param prettyPrint if true, try to produce a nicer text layout, when possible.
   * Otherwise, use the default options of the serializer.
   * Default value is false as not all serializers and formats support pretty printing.
   * @return a string that contains human-readable representation of the input configuration.
   */
  public static String toString(final Configuration config, final boolean prettyPrint) {
    return SERIALIZER.toString(config, prettyPrint);
  }
}
