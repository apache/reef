/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.io.TempFileCreator;
import com.microsoft.reef.io.WorkingDirectoryTempFileCreator;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.files.JobJarMaker;
import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.runtime.common.parameters.DeleteTempFiles;
import com.microsoft.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import com.microsoft.reef.util.JARFileMaker;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

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

  private final String jobSubmissionDirectory;
  private final Map<String, LocalResource> globalResources;
  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;
  private final FileSystem fileSystem;
  private final TempFileCreator tempFileCreator;
  private final boolean deleteTempFiles;

  @Inject
  EvaluatorSetupHelper(
      final @Parameter(JobSubmissionDirectory.class) String jobSubmissionDirectory,
      final YarnConfiguration yarnConfiguration,
      final REEFFileNames fileNames,
      final ConfigurationSerializer configurationSerializer,
      final TempFileCreator tempFileCreator,
      final @Parameter(DeleteTempFiles.class) boolean deleteTempFiles) throws IOException {
    this.tempFileCreator = tempFileCreator;
    this.deleteTempFiles = deleteTempFiles;

    this.fileSystem = FileSystem.get(yarnConfiguration);
    this.jobSubmissionDirectory = jobSubmissionDirectory;
    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;
    this.globalResources = this.setup();
  }

  public Map<String, LocalResource> getGlobalResources() {
    return this.globalResources;
  }

  private Map<String, LocalResource> setup() throws IOException {
    final Map<String, LocalResource> result = new HashMap<>(1);
    final Path pathToGlobalJar = this.uploadToJobFolder(makeGlobalJar());
    result.put(this.fileNames.getGlobalFolderPath(), makeLocalResourceForJarFile(pathToGlobalJar));
    return result;
  }

  private File makeGlobalJar() throws IOException {
    final File jarFile = new File(
        this.fileNames.getGlobalFolderName() + this.fileNames.getJarFileSuffix());
    new JARFileMaker(jarFile).addChildren(this.fileNames.getGlobalFolder()).close();
    return jarFile;
  }

  /**
   * Sets up the LocalResources for a new Evaluator.
   *
   * @param resourceLaunchProto
   * @return
   * @throws IOException
   */
  public Map<String, LocalResource> getResources(
      final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto)
      throws IOException {

    final Map<String, LocalResource> result = new HashMap<>();
    result.putAll(getGlobalResources());

    final File localStagingFolder = this.tempFileCreator.createTempDirectory(this.fileNames.getEvaluatorFolderPrefix());

    // Write the configuration
    final File configurationFile = new File(
        localStagingFolder, this.fileNames.getEvaluatorConfigurationName());
    this.configurationSerializer.toFile(
        makeEvaluatorConfiguration(resourceLaunchProto), configurationFile);

    // Copy files to the staging folder
    JobJarMaker.copy(resourceLaunchProto.getFileList(), localStagingFolder);

    // Make a JAR file out of it
    final File localFile = tempFileCreator.createTempFile(
        this.fileNames.getEvaluatorFolderPrefix(), this.fileNames.getJarFileSuffix());
    new JARFileMaker(localFile).addChildren(localStagingFolder).close();

    // Upload the JAR to the job folder
    final Path pathToEvaluatorJar = uploadToJobFolder(localFile);
    result.put(this.fileNames.getLocalFolderPath(), makeLocalResourceForJarFile(pathToEvaluatorJar));

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
   * @param resourceLaunchProto
   * @return
   * @throws IOException
   */

  private Configuration makeEvaluatorConfiguration(
      final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) throws IOException {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(this.configurationSerializer.fromString(resourceLaunchProto.getEvaluatorConf()))
        .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)
        .build();
  }

  /**
   * Uploads the given file to the job folder on (H)DFS.
   *
   * @param file
   * @return
   * @throws IOException
   */
  private Path uploadToJobFolder(final File file) throws IOException {
    final Path source = new Path(file.getAbsolutePath());
    final Path destination = new Path(this.jobSubmissionDirectory + "/" + file.getName());
    LOG.log(Level.FINE, "Uploading {0} to {1}", new Object[]{source, destination});
    this.fileSystem.copyFromLocalFile(false, true, source, destination);
    return destination;
  }

  /**
   * Creates a LocalResource instance for the JAR file referenced by the given Path
   *
   * @param path
   * @return
   * @throws IOException
   */
  private LocalResource makeLocalResourceForJarFile(final Path path) throws IOException {
    final LocalResource localResource = Records.newRecord(LocalResource.class);
    final FileStatus status = FileContext.getFileContext(this.fileSystem.getUri()).getFileStatus(path);
    localResource.setType(LocalResourceType.ARCHIVE);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(status.getPath()));
    localResource.setTimestamp(status.getModificationTime());
    localResource.setSize(status.getLen());
    return localResource;
  }
}
