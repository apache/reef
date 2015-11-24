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

import org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Tests for generating Driver configuration by bootstrapping the process to the
 * {@link YarnBootstrapREEFLauncher}. Tests the Avro job submission serialization
 * deserialization from C# using mocked, expected Strings.
 */
public final class TestAvroJobSubmissionParametersSerializationFromCS {
  private static final String NULL_REP = "NULL";
  private static final String STRING_REP = "HelloREEF";
  private static final String STRING_REP_QUOTED = "\"" + STRING_REP + "\"";
  private static final long NUMBER_REP = 12345;
  private static final String AVRO_YARN_PARAMETERS_SERIALIZED_STRING =
      "{" +
          "\"sharedJobSubmissionParameters\":" +
          "{" +
            "\"jobId\":" + STRING_REP_QUOTED + "," +
            "\"tcpBeginPort\":" + NUMBER_REP + "," +
            "\"tcpRangeCount\":" + NUMBER_REP + "," +
            "\"tcpTryCount\":" + NUMBER_REP + "," +
            "\"jobSubmissionFolder\":" + STRING_REP_QUOTED +
          "}," +
          "\"driverMemory\":" + NUMBER_REP + "," +
          "\"driverRecoveryTimeout\":" + NUMBER_REP + "," +
          "\"dfsJobSubmissionFolder\":\"" + STRING_REP + "\"," +
          "\"jobSubmissionDirectoryPrefix\":" + STRING_REP_QUOTED +
      "}";

  private static final String AVRO_YARN_CLUSTER_PARAMETERS_SERIALIZED_STRING =
      "{" +
          "\"yarnJobSubmissionParameters\":" + AVRO_YARN_PARAMETERS_SERIALIZED_STRING + "," +
          "\"maxApplicationSubmissions\":" + NUMBER_REP + "," +
          "\"securityTokenKind\":\"" + NULL_REP + "\"," +
          "\"securityTokenService\":\"" + NULL_REP + "\"" +
      "}";

  /**
   * Tests deserialization of the Avro parameters for submission from the cluster from C#.
   * @throws IOException
   */
  @Test
  public void testAvroYarnClusterParametersDeserialization() throws IOException {
    final YarnClusterSubmissionFromCS yarnClusterSubmissionFromCS = createYarnClusterSubmissionFromCS();
    assert yarnClusterSubmissionFromCS.getDriverFolder().equals(new File(STRING_REP));
    assert yarnClusterSubmissionFromCS.getDriverMemory() == NUMBER_REP;
    assert yarnClusterSubmissionFromCS.getDriverRecoveryTimeout() == NUMBER_REP;
    assert yarnClusterSubmissionFromCS.getJobId().equals(STRING_REP);
    assert yarnClusterSubmissionFromCS.getMaxApplicationSubmissions() == NUMBER_REP;
    assert yarnClusterSubmissionFromCS.getTokenKind().equals(NULL_REP);
    assert yarnClusterSubmissionFromCS.getTokenService().equals(NULL_REP);

    verifyYarnJobSubmissionParams(yarnClusterSubmissionFromCS.getYarnJobSubmissionParameters());
  }

  /**
   * Tests deserialization of the Avro parameters for YARN from C#.
   * @throws IOException
   */
  @Test
  public void testAvroYarnParametersDeserialization() throws IOException {
    verifyYarnJobSubmissionParams(createAvroYarnJobSubmissionParameters());
  }

  /**
   * Tests a round-trip serialization deserialization process of the Avro parameters from C#.
   * @throws IOException
   */
  @Test
  public void testAvroYarnParametersSerialization() throws IOException {
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      YarnJobSubmissionParametersFileGenerator.writeAvroYarnJobSubmissionParametersToOutputStream(
          createYarnClusterSubmissionFromCS(), STRING_REP, outputStream);
      final byte[] content = outputStream.toByteArray();
      try (final InputStream stream = new ByteArrayInputStream(content)) {
        verifyYarnJobSubmissionParams(
            YarnBootstrapDriverConfigGenerator.readYarnJobSubmissionParametersFromInputStream(stream));
      }
    }
  }

  /**
   * Tests that the DriverConfiguration is bound with Avro parameters from C#.
   * @throws IOException
   * @throws InjectionException
   */
  @Test
  public void testYarnBootstrapDriverConfigGenerator() throws IOException, InjectionException {
    final Configuration yarnBootstrapDriverConfig =
        YarnBootstrapDriverConfigGenerator.getYarnDriverConfiguration(createAvroYarnJobSubmissionParameters());
    final Injector injector = Tang.Factory.getTang().newInjector(yarnBootstrapDriverConfig);

    assert injector.getNamedInstance(JobSubmissionDirectory.class).equals(STRING_REP);
    assert injector.getNamedInstance(JobSubmissionDirectoryPrefix.class).equals(STRING_REP);
    assert injector.getNamedInstance(JobIdentifier.class).equals(STRING_REP);
    assert injector.getNamedInstance(TcpPortRangeBegin.class) == NUMBER_REP;
    assert injector.getNamedInstance(TcpPortRangeCount.class) == NUMBER_REP;
    assert injector.getNamedInstance(TcpPortRangeTryCount.class) == NUMBER_REP;
  }

  private static AvroYarnJobSubmissionParameters createAvroYarnJobSubmissionParameters() throws IOException {
    try (final InputStream stream =
             new ByteArrayInputStream(AVRO_YARN_PARAMETERS_SERIALIZED_STRING.getBytes(StandardCharsets.UTF_8))) {
      return YarnBootstrapDriverConfigGenerator.readYarnJobSubmissionParametersFromInputStream(stream);
    }
  }

  private static YarnClusterSubmissionFromCS createYarnClusterSubmissionFromCS() throws IOException {
    try (final InputStream stream =
             new ByteArrayInputStream(
                 AVRO_YARN_CLUSTER_PARAMETERS_SERIALIZED_STRING.getBytes(StandardCharsets.UTF_8))) {
      return YarnClusterSubmissionFromCS.readYarnClusterSubmissionFromCSFromInputStream(stream);
    }
  }

  private static void verifyYarnJobSubmissionParams(final AvroYarnJobSubmissionParameters jobSubmissionParameters) {
    final AvroJobSubmissionParameters sharedJobSubmissionParams =
        jobSubmissionParameters.getSharedJobSubmissionParameters();
    assert sharedJobSubmissionParams.getJobId().toString().equals(STRING_REP);
    assert sharedJobSubmissionParams.getJobSubmissionFolder().toString().equals(STRING_REP);
    assert sharedJobSubmissionParams.getTcpBeginPort() == NUMBER_REP;
    assert sharedJobSubmissionParams.getTcpRangeCount() == NUMBER_REP;
    assert sharedJobSubmissionParams.getTcpTryCount() == NUMBER_REP;
    assert jobSubmissionParameters.getDfsJobSubmissionFolder().toString().equals(STRING_REP);
    assert jobSubmissionParameters.getJobSubmissionDirectoryPrefix().toString().equals(STRING_REP);
  }
}
