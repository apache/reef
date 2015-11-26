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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.parameters.DeleteTempFiles;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.JARFileMaker;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the global files on the driver and can produce the needed FileResources for container submissions.
 */
@DriverSide
final class EvaluatorSetupHelper {

  private static final Logger LOG = Logger.getLogger(EvaluatorSetupHelper.class.getName());

  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;
  private final TempFileCreator tempFileCreator;
  private final UploaderToJobFolder uploader;
  private final GlobalJarUploader globalJarUploader;
  private final boolean deleteTempFiles;

  @Inject
  EvaluatorSetupHelper(
      final REEFFileNames fileNames,
      final ConfigurationSerializer configurationSerializer,
      final TempFileCreator tempFileCreator,
      @Parameter(DeleteTempFiles.class) final boolean deleteTempFiles,
      final UploaderToJobFolder uploader,
      final GlobalJarUploader globalJarUploader) throws IOException {
    this.tempFileCreator = tempFileCreator;
    this.deleteTempFiles = deleteTempFiles;
    this.globalJarUploader = globalJarUploader;

    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;
    this.uploader = uploader;
  }

  /**
   * @return the map to be used in formulating the evaluator launch submission.
   */
  Map<String, LocalResource> getGlobalResources() {
    try {
      return this.globalJarUploader.call();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to upload the global JAR file to the job folder.", e);
    }
  }


  /**
   * Sets up the LocalResources for a new Evaluator.
   *
   * @param resourceLaunchEvent
   * @return
   * @throws IOException
   */
  Map<String, LocalResource> getResources(
      final ResourceLaunchEvent resourceLaunchEvent)
      throws IOException {

    final Map<String, LocalResource> result = new HashMap<>();
    result.putAll(getGlobalResources());

    final File localStagingFolder = this.tempFileCreator.createTempDirectory(this.fileNames.getEvaluatorFolderPrefix());

    // Write the configuration
    final File configurationFile = new File(localStagingFolder, this.fileNames.getEvaluatorConfigurationName());
    this.configurationSerializer.toFile(makeEvaluatorConfiguration(resourceLaunchEvent), configurationFile);

    // Copy files to the staging folder
    JobJarMaker.copy(resourceLaunchEvent.getFileSet(), localStagingFolder);

    // Make a JAR file out of it
    final File localFile = tempFileCreator.createTempFile(
        this.fileNames.getEvaluatorFolderPrefix(), this.fileNames.getJarFileSuffix());
    new JARFileMaker(localFile).addChildren(localStagingFolder).close();

    // Upload the JAR to the job folder
    final Path pathToEvaluatorJar = this.uploader.uploadToJobFolder(localFile);
    result.put(this.fileNames.getLocalFolderPath(), this.uploader.makeLocalResourceForJarFile(pathToEvaluatorJar));

    if (this.deleteTempFiles) {
      LOG.log(Level.FINE, "Marking [{0}] for deletion at the exit of this JVM and deleting [{1}]",
          new Object[]{localFile.getAbsolutePath(), localStagingFolder.getAbsolutePath()});
      localFile.deleteOnExit();
      localStagingFolder.delete();
    } else {
      LOG.log(Level.FINE, "The evaluator staging folder will be kept at [{0}], the JAR at [{1}]",
          new Object[]{localFile.getAbsolutePath(), localStagingFolder.getAbsolutePath()});
    }
    return result;
  }

  /**
   * Assembles the configuration for an Evaluator.
   *
   * @param resourceLaunchEvent
   * @return
   * @throws IOException
   */

  private Configuration makeEvaluatorConfiguration(final ResourceLaunchEvent resourceLaunchEvent)
      throws IOException {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(resourceLaunchEvent.getEvaluatorConf())
        .build();
  }
}
