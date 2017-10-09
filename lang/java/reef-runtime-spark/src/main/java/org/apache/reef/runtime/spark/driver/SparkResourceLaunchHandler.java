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
package org.apache.reef.runtime.spark.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.spark.util.EvaluatorLaunch;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
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
final class SparkResourceLaunchHandler implements ResourceLaunchHandler {
  private final ConfigurationSerializer configurationSerializer;
  private final RemoteManager remoteManager;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final double jvmHeapFactor;
  private final REEFExecutors executors;
  private static final Logger LOG = Logger.getLogger(SparkResourceLaunchHandler.class.getName());

  @Inject
  SparkResourceLaunchHandler(final ConfigurationSerializer configurationSerializer,
                             final RemoteManager remoteManager,
                             final REEFFileNames fileNames,
                             final REEFExecutors executors,
                             final ClasspathProvider classpath,
                             @Parameter(JVMHeapSlack.class) final double jvmHeapSlack) {
    this.configurationSerializer = configurationSerializer;
    this.remoteManager = remoteManager;
    this.fileNames = fileNames;
    this.executors = executors;
    this.classpath = classpath;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
  }


  @Override
  public void onNext(final ResourceLaunchEvent resourceLaunchEvent) {
    try {
      LOG.log(Level.INFO, "resourceLaunch. {0}", resourceLaunchEvent.toString());

      final File localStagingFolder =
          Files.createTempDirectory(this.fileNames.getEvaluatorFolderPrefix()).toFile();

      final Configuration evaluatorConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(resourceLaunchEvent.getEvaluatorConf())
          .build();

      final File configurationFile = new File(
          localStagingFolder, this.fileNames.getEvaluatorConfigurationName());
      this.configurationSerializer.toFile(evaluatorConfiguration, configurationFile);

      JobJarMaker.copy(resourceLaunchEvent.getFileSet(), localStagingFolder);

      final FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
      final Path hdfsFolder = new Path(fileSystem.getUri() + "/" + resourceLaunchEvent.getIdentifier() + "/");
      FileUtil.copy(localStagingFolder, fileSystem, hdfsFolder, false, new org.apache.hadoop.conf.Configuration());

      // TODO[JIRA REEF-102]: Replace REEFExecutor with a simple launch command (we only need to launch REEFExecutor)
      final List<String> command =
          getLaunchCommand(resourceLaunchEvent, this.executors.getMemory(resourceLaunchEvent.getIdentifier()));
      this.executors.launchEvaluator(
          new EvaluatorLaunch(resourceLaunchEvent.getIdentifier(), StringUtils.join(command, ' ')));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getLaunchCommand(final ResourceLaunchEvent resourceLaunchEvent,
                                        final int executorMemory) {
    final EvaluatorProcess process = resourceLaunchEvent.getProcess()
        .setConfigurationFileName(this.fileNames.getEvaluatorConfigurationPath());

    if (process.isOptionSet()) {
      return process.getCommandLine();
    } else {
      return process
          .setMemory((int) (this.jvmHeapFactor * executorMemory))
          .getCommandLine();
    }
  }
}
