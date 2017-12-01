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
package org.apache.reef.bridge.client;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.reef.bridge.client.avro.AvroAzureBatchJobSubmissionParameters;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the Java Driver configuration generator for .NET Drivers that generates
 * the Driver configuration at runtime. Called by {@link AzureBatchBootstrapREEFLauncher}.
 */
final class AzureBatchBootstrapDriverConfigGenerator {

  private static final Logger LOG = Logger.getLogger(AzureBatchBootstrapDriverConfigGenerator.class.getName());

  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  private AzureBatchBootstrapDriverConfigGenerator(final DriverConfigurationProvider driverConfigurationProvider) {
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  Configuration getDriverConfigurationFromParams(final String bootstrapJobArgsLocation) throws IOException {

    final File bootstrapJobArgsFile = new File(bootstrapJobArgsLocation).getCanonicalFile();

    final AvroAzureBatchJobSubmissionParameters azureBatchBootstrapJobArgs =
        readAzureBatchJobSubmissionParametersFromFile(bootstrapJobArgsFile);

    final String jobId = azureBatchBootstrapJobArgs.getSharedJobSubmissionParameters().getJobId().toString();
    final File jobFolder = new File(azureBatchBootstrapJobArgs
        .getSharedJobSubmissionParameters().getJobSubmissionFolder().toString());

    LOG.log(Level.INFO, "jobFolder {0} jobId {1}.", new Object[]{jobFolder.toURI(), jobId});

    return this.driverConfigurationProvider.getDriverConfiguration(
        jobFolder.toURI(), ClientRemoteIdentifier.NONE, jobId,
        Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER);
  }

  private AvroAzureBatchJobSubmissionParameters readAzureBatchJobSubmissionParametersFromFile(final File file)
      throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(file)) {
      return readAzureBatchSubmissionParametersFromInputStream(fileInputStream);
    }
  }

  private static AvroAzureBatchJobSubmissionParameters readAzureBatchSubmissionParametersFromInputStream(
      final InputStream inputStream) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
        AvroAzureBatchJobSubmissionParameters.getClassSchema(), inputStream);
    final SpecificDatumReader<AvroAzureBatchJobSubmissionParameters> reader = new SpecificDatumReader<>(
        AvroAzureBatchJobSubmissionParameters.class);
    return reader.read(null, decoder);
  }
}
