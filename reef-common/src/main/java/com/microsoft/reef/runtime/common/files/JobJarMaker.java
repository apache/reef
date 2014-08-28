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
package com.microsoft.reef.runtime.common.files;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.annotations.audience.RuntimeAuthor;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.util.JARFileMaker;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility that takes a JobSubmissionProto and turns it into a Job Submission Jar.
 */
@Private
@RuntimeAuthor
@ClientSide
public final class JobJarMaker {

  private static final Logger LOG = Logger.getLogger(JobJarMaker.class.getName());

  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;

  @Inject
  JobJarMaker(final ConfigurationSerializer configurationSerializer,
              final REEFFileNames fileNames) {
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
  }

  public File createJobSubmissionJAR(
      final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto,
      final Configuration driverConfiguration) throws IOException {

    // Copy all files to a local job submission folder
    final File jobSubmissionFolder = makejobSubmissionFolder();
    LOG.log(Level.FINE, "Staging submission in {0}", jobSubmissionFolder);

    final File localFolder = new File(jobSubmissionFolder, this.fileNames.getLocalFolderName());
    final File globalFolder = new File(jobSubmissionFolder, this.fileNames.getGlobalFolderName());

    this.copy(jobSubmissionProto.getGlobalFileList(), globalFolder);
    this.copy(jobSubmissionProto.getLocalFileList(), localFolder);

    // Store the Driver Configuration in the JAR file.
    this.configurationSerializer.toFile(
        driverConfiguration, new File(localFolder, this.fileNames.getDriverConfigurationName()));

    // Create a JAR File for the submission
    final File jarFile = File.createTempFile(
        this.fileNames.getJobFolderPrefix(), this.fileNames.getJarFileSuffix());

    LOG.log(Level.FINE, "Creating job submission jar file: {0}", jarFile);
    new JARFileMaker(jarFile).addChildren(jobSubmissionFolder).close();

    return jarFile;
  }

  public static void copy(final Iterable<ReefServiceProtos.FileResourceProto> files, final File destinationFolder) {

    if (!destinationFolder.exists()) {
      destinationFolder.mkdirs();
    }

    for (final ReefServiceProtos.FileResourceProto fileProto : files) {
      final File sourceFile = toFile(fileProto);
      final File destinationFile = new File(destinationFolder, fileProto.getName());
      if (destinationFile.exists()) {
        LOG.log(Level.FINEST,
            "Will not add {0} to the job jar because another file with the same name was already added.",
            sourceFile.getAbsolutePath()
        );
      } else {
        try {
          java.nio.file.Files.copy(sourceFile.toPath(), destinationFile.toPath());
        } catch (IOException e) {
          throw new RuntimeException("Couldn't copy a file to the job folder", e);
        }
      }
    }
  }

  private static File toFile(final ReefServiceProtos.FileResourceProto fileProto) {
    return new File(fileProto.getPath());
  }

  private File makejobSubmissionFolder() throws IOException {
    return Files.createTempDirectory(this.fileNames.getJobFolderPrefix()).toFile();
  }
}
