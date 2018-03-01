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
package org.apache.reef.runtime.common.files;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.parameters.DeleteTempFiles;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.JARFileMaker;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
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
  private final boolean deleteTempFilesOnExit;

  @Inject
  JobJarMaker(final ConfigurationSerializer configurationSerializer,
              final REEFFileNames fileNames,
              @Parameter(DeleteTempFiles.class) final boolean deleteTempFilesOnExit) {
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.deleteTempFilesOnExit = deleteTempFilesOnExit;
  }

  public JarBuilder newBuilder() {
    return new JarBuilder();
  }

  public static void copy(final Iterable<FileResource> files, final File destinationFolder) {

    if (!destinationFolder.exists() && !destinationFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", destinationFolder.getAbsolutePath());
    }

    for (final FileResource fileProto : files) {
      final File sourceFile = toFile(fileProto);
      final File destinationFile = new File(destinationFolder, fileProto.getName());
      if (destinationFile.exists()) {
        LOG.log(Level.FINEST,
            "Will not add {0} to the job jar because another file with the same name was already added.",
            sourceFile.getAbsolutePath()
        );
      } else {
        try {
          Files.copy(sourceFile.toPath(), destinationFile.toPath());
        } catch (final IOException e) {
          final String message = new StringBuilder("Copy of file [")
              .append(sourceFile.getAbsolutePath())
              .append("] to [")
              .append(destinationFile.getAbsolutePath())
              .append("] failed.")
              .toString();
          throw new RuntimeException(message, e);
        }
      }
    }
  }

  private static File toFile(final FileResource fileProto) {
    return new File(fileProto.getPath());
  }

  public File createJobSubmissionJAR(
      final JobSubmissionEvent jobSubmissionEvent,
      final Configuration driverConfiguration) throws IOException {

    return new JarBuilder()
        .withConfiguration(driverConfiguration)
        .withGlobalFileSet(jobSubmissionEvent.getGlobalFileSet())
        .withLocalFileSet(jobSubmissionEvent.getLocalFileSet())
        .withConfigurationFileName(this.fileNames.getDriverConfigurationName())
        .build();
  }

  private File makejobSubmissionFolder() throws IOException {
    return Files.createTempDirectory(this.fileNames.getJobFolderPrefix()).toFile();
  }

  /**
   * Builder class for building JAR files.
   */
  public final class JarBuilder {

    private Set<FileResource> localFiles = new HashSet<>();
    private Set<FileResource> globalFiles = new HashSet<>();
    private Configuration configuration = null;
    private String configurationFilename = null;

    private JarBuilder() {}

    public JarBuilder withLocalFileSet(final Set<FileResource> withLocalFiles) {
      this.localFiles = withLocalFiles;
      return this;
    }

    public JarBuilder withGlobalFileSet(final Set<FileResource> withGlobalFiles) {
      this.globalFiles = withGlobalFiles;
      return this;
    }

    public JarBuilder withConfiguration(final Configuration withConfiguration) {
      this.configuration = withConfiguration;
      return this;
    }

    public JarBuilder withConfigurationFileName(final String withConfigurationFilename) {
      this.configurationFilename = withConfigurationFilename;
      return this;
    }

    public File build() throws IOException {
      // Copy all files to a local job submission folder
      final File jobSubmissionFolder = makejobSubmissionFolder();
      LOG.log(Level.FINE, "Staging submission in {0}", jobSubmissionFolder);

      final File localFolder = new File(jobSubmissionFolder, JobJarMaker.this.fileNames.getLocalFolderName());
      final File globalFolder = new File(jobSubmissionFolder, JobJarMaker.this.fileNames.getGlobalFolderName());

      JobJarMaker.copy(this.globalFiles, globalFolder);
      JobJarMaker.copy(this.localFiles, localFolder);

      // Store the Configuration in the JAR file.
      if (configuration != null && StringUtils.isNotBlank(this.configurationFilename)) {
        JobJarMaker.this.configurationSerializer
            .toFile(configuration, new File(localFolder, this.configurationFilename));
      }

      // Create a JAR File for the submission
      final File jarFile = File.createTempFile(JobJarMaker.this.fileNames.getJobFolderPrefix(),
          JobJarMaker.this.fileNames.getJarFileSuffix());

      LOG.log(Level.FINE, "Creating job submission jar file: {0}", jarFile);
      new JARFileMaker(jarFile).addChildren(jobSubmissionFolder).close();

      if (JobJarMaker.this.deleteTempFilesOnExit) {
        LOG.log(Level.FINE,
            "Deleting the temporary job folder [{0}] and marking the jar file [{1}] for deletion after the JVM exits.",
            new Object[]{jobSubmissionFolder.getAbsolutePath(), jarFile.getAbsolutePath()});
        if (!jobSubmissionFolder.delete()) {
          LOG.log(Level.WARNING, "Failed to delete [{0}]", jobSubmissionFolder.getAbsolutePath());
        }
        jarFile.deleteOnExit();
      } else {
        LOG.log(Level.FINE, "Keeping the temporary job folder [{0}] and jar file [{1}] available after job submission.",
            new Object[]{jobSubmissionFolder.getAbsolutePath(), jarFile.getAbsolutePath()});
      }
      return jarFile;
    }
  }
}
