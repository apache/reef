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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * Does client side manipulation of driver configuration for YARN runtime.
 */
final class YarnJobSubmissionParametersFileGenerator {
  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionParametersFileGenerator.class.getName());
  private final REEFFileNames fileNames;

  @Inject
  private YarnJobSubmissionParametersFileGenerator(final REEFFileNames fileNames) {
    this.fileNames = fileNames;
  }

  /**
   * Writes driver configuration to disk.
   * @param yarnClusterSubmissionFromCS the information needed to submit encode YARN parameters and create the
   *                                    YARN job for submission from the cluster.
   * @throws IOException
   */
  public void writeConfiguration(final YarnClusterSubmissionFromCS yarnClusterSubmissionFromCS,
                                 final JobFolder jobFolderOnDFS) throws IOException {
    final File yarnParametersFile = new File(yarnClusterSubmissionFromCS.getDriverFolder(),
        fileNames.getYarnBootstrapParamFilePath());

    try (final FileOutputStream fileOutputStream = new FileOutputStream(yarnParametersFile)) {
      // this is mainly a test hook.
      writeAvroYarnJobSubmissionParametersToOutputStream(
          yarnClusterSubmissionFromCS, jobFolderOnDFS.getPath().toString(), fileOutputStream);
    }
  }

  static void writeAvroYarnJobSubmissionParametersToOutputStream(
      final YarnClusterSubmissionFromCS yarnClusterSubmissionFromCS,
      final String jobFolderOnDFSPath,
      final OutputStream outputStream) throws IOException {
    final DatumWriter<AvroYarnJobSubmissionParameters> datumWriter =
        new SpecificDatumWriter<>(AvroYarnJobSubmissionParameters.class);

    final AvroYarnJobSubmissionParameters jobSubmissionParameters =
        yarnClusterSubmissionFromCS.getYarnJobSubmissionParameters();
    jobSubmissionParameters.setDfsJobSubmissionFolder(jobFolderOnDFSPath);
    final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(jobSubmissionParameters.getSchema(),
        outputStream);
    datumWriter.write(jobSubmissionParameters, encoder);
    encoder.flush();
    outputStream.flush();
  }
}
