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
package org.apache.reef.runtime.azbatch.util;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract implementation of the OS command builder.
 */
public abstract class AbstractCommandBuilder implements CommandBuilder {

  private final Class launcherClass;
  private final Class shimLauncherClass;
  private final List<String> commandListPrefix;
  private final String osCommandFormat;
  private final RuntimePathProvider runtimePathProvider;

  protected final ClasspathProvider classpathProvider;
  protected final REEFFileNames reefFileNames;
  protected final AzureBatchFileNames azureBatchFileNames;

  AbstractCommandBuilder(
      final Class launcherClass,
      final Class shimLauncherClass,
      final List<String> commandListPrefix,
      final String osCommandFormat,
      final ClasspathProvider classpathProvider,
      final RuntimePathProvider runtimePathProvider,
      final REEFFileNames reefFileNames,
      final AzureBatchFileNames azureBatchFileNames) {
    this.launcherClass = launcherClass;
    this.shimLauncherClass = shimLauncherClass;
    this.commandListPrefix = commandListPrefix;
    this.osCommandFormat = osCommandFormat;

    this.classpathProvider = classpathProvider;
    this.reefFileNames = reefFileNames;
    this.azureBatchFileNames = azureBatchFileNames;
    this.runtimePathProvider = runtimePathProvider;
  }

  public String buildDriverCommand(final JobSubmissionEvent jobSubmissionEvent) {
    List<String> commandList = new JavaLaunchCommandBuilder(this.launcherClass, this.commandListPrefix)
        .setJavaPath(runtimePathProvider.getPath())
        .setConfigurationFilePaths(Collections.singletonList(this.reefFileNames.getDriverConfigurationPath()))
        .setClassPath(getDriverClasspath())
        .setMemory(jobSubmissionEvent.getDriverMemory().get())
        .build();
    return String.format(this.osCommandFormat, StringUtils.join(commandList, ' '));
  }

  public String buildEvaluatorShimCommand(final int evaluatorShimMemory, final String configurationPath) {
    List<String> commandList = new JavaLaunchCommandBuilder(this.shimLauncherClass, this.commandListPrefix)
        .setJavaPath(runtimePathProvider.getPath())
        .setConfigurationFilePaths(Collections.singletonList(configurationPath))
        .setClassPath(getEvaluatorShimClasspath())
        .setMemory(evaluatorShimMemory)
        .build();
    return String.format(this.osCommandFormat, StringUtils.join(commandList, ' '));
  }

  public String buildEvaluatorCommand(final ResourceLaunchEvent resourceLaunchEvent,
                                      final int containerMemory, final double jvmHeapFactor) {
    List<String> commandList = new ArrayList<>();

    // Use EvaluatorProcess to be compatible with JVMProcess and CLRProcess
    final EvaluatorProcess process = resourceLaunchEvent.getProcess()
        .setConfigurationFileName(this.reefFileNames.getEvaluatorConfigurationPath());

    if (process.isOptionSet()) {
      commandList.addAll(process.getCommandLine());
    } else {
      commandList.addAll(process.setMemory((int) (jvmHeapFactor * containerMemory)).getCommandLine());
    }

    return StringUtils.join(commandList, ' ');
  }

  /**
   * Returns the driver classpath string which is compatible with the intricacies of the OS.
   *
   * @return classpath parameter string.
   */
  protected abstract String getDriverClasspath();

  /**
   * Returns the evaluator shim classpath string which is compatible with the intricacies of the OS.
   *
   * @return classpath parameter string.
   */
  protected abstract String getEvaluatorShimClasspath();
}
