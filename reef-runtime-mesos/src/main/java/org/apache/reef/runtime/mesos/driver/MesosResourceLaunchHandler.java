/**
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
package org.apache.reef.runtime.mesos.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.WorkingDirectoryTempFileCreator;
import org.apache.reef.proto.DriverRuntimeProtocol;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.common.launch.LaunchCommandBuilder;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.EvaluatorLaunchProto;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
@Private
final class MesosResourceLaunchHandler implements ResourceLaunchHandler {
  private final ConfigurationSerializer configurationSerializer;
  private final RemoteManager remoteManager;
  private final Executors executors;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;

  @Inject
  MesosResourceLaunchHandler(final ConfigurationSerializer configurationSerializer,
                             final RemoteManager remoteManager,
                             final Executors executors,
                             final REEFFileNames fileNames,
                             final ClasspathProvider classpath) {
    this.configurationSerializer = configurationSerializer;
    this.remoteManager = remoteManager;
    this.executors = executors;
    this.fileNames = fileNames;
    this.classpath = classpath;
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    try {
      final File localStagingFolder =
          Files.createTempDirectory(this.fileNames.getEvaluatorFolderPrefix()).toFile();

      final Configuration evaluatorConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(this.configurationSerializer.fromString(resourceLaunchProto.getEvaluatorConf()))
          .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)
          .build();

      final File configurationFile = new File(
          localStagingFolder, this.fileNames.getEvaluatorConfigurationName());
      this.configurationSerializer.toFile(evaluatorConfiguration, configurationFile);

      JobJarMaker.copy(resourceLaunchProto.getFileList(), localStagingFolder);

      final FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
      final Path hdfsFolder = new Path(fileSystem.getUri() + "/" + resourceLaunchProto.getIdentifier() + "/");
      FileUtil.copy(localStagingFolder, fileSystem, hdfsFolder, false, new org.apache.hadoop.conf.Configuration());

      final LaunchCommandBuilder commandBuilder;
      switch (resourceLaunchProto.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder().setClassPath(this.classpath.getEvaluatorClasspath());
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException("Unsupported container type");
      }

      final List<String> command = commandBuilder
          .setErrorHandlerRID(this.remoteManager.getMyIdentifier())
          .setLaunchID(resourceLaunchProto.getIdentifier())
          .setConfigurationFileName(this.fileNames.getEvaluatorConfigurationPath())
          .setMemory(this.executors.getEvaluatorMemory(resourceLaunchProto.getIdentifier()))
          .build();

      this.executors.launchEvaluator(resourceLaunchProto.getIdentifier(),
          EvaluatorLaunchProto.newBuilder()
              .setCommand(StringUtils.join(command, ' '))
              .build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}