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
package org.apache.reef.runtime.multi.client;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.multi.driver.MultiRuntimeDriverConfiguration;
import org.apache.reef.runtime.multi.utils.RuntimeDefinition;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

/**
 * Provides base class for driver configuration providers for multi runtimes.
 */
abstract class AbstractDriverConfigurationProvider implements DriverConfigurationProvider {

  private static final String CHARSET_NAME = "ISO-8859-1";

  protected static String serializeConfiguration(final ConfigurationModule configModule) {
    final Configuration localDriverConfiguration = configModule.build();
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    return serializer.toString(localDriverConfiguration);
  }

  protected static String serializeRuntimeDefinition(final String serializedConfig,
                                                     final boolean defaultRuntime,
                                                     final String runtimeName) {
    RuntimeDefinition rd =
            new RuntimeDefinition(runtimeName, serializedConfig, defaultRuntime);
    final DatumWriter<RuntimeDefinition> configurationWriter =
            new SpecificDatumWriter<>(RuntimeDefinition.class);
    final String serializedConfiguration;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(RuntimeDefinition.SCHEMA$, out);
      configurationWriter.write(rd, encoder);
      encoder.flush();
      out.flush();
      serializedConfiguration = out.toString(CHARSET_NAME);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return serializedConfiguration;
  }

  private static Configuration generateConfigurationModule(final ArrayList<String> serializedConfigurations,
                                                           final String jobId,
                                                           final String clientRemoteId) {
    ConfigurationModule module = MultiRuntimeDriverConfiguration.CONF
            .set(MultiRuntimeDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(MultiRuntimeDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId);
    for (String config : serializedConfigurations) {
      module = module.set(MultiRuntimeDriverConfiguration.SERIALIZED_RUNTIME_DEFINITION, config);
    }

    return module.build();
  }

  /**
   * Generates needed driver configuration modules.
   *
   * @param jobFolder      the job folder
   * @param clientRemoteId the client remote id
   * @param jobId          the job id
   * @return array of configuration modules for runtimes, when the first element contains the default runtime
   */
  protected abstract String[] getDriverConfiguration(final URI jobFolder,
                                                     final String clientRemoteId,
                                                     final String jobId);

  /**
   * Assembles the driver configuration.
   *
   * @param jobFolder                The folder in which the local runtime will execute this job.
   * @param clientRemoteId           the remote identifier of the client. It is used by the Driver to establish a
   *                                 connection back to the client.
   * @param jobId                    The identifier of the job.
   * @param applicationConfiguration The configuration of the application, e.g. a filled out DriverConfiguration
   * @return The Driver configuration to be used to instantiate the Driver.
   */
  public final Configuration getDriverConfiguration(final URI jobFolder,
                                                    final String clientRemoteId,
                                                    final String jobId,
                                                    final Configuration applicationConfiguration) {
    String[] configurationModules = getDriverConfiguration(jobFolder, clientRemoteId, jobId);
    ArrayList<String> runtimes = new ArrayList<>();
    for (String module : configurationModules) {
      runtimes.add(module);
    }

    return Configurations.merge(generateConfigurationModule(runtimes, jobId, clientRemoteId), applicationConfiguration);
  }
}
