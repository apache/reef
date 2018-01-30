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
package org.apache.reef.runime.azbatch.util;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Abstract implementation of the OS command builder.
 */
public abstract class AbstractCommandBuilder implements CommandBuilder {

  public static final String STD_OUT_FILE = "stdout.txt";
  public static final String STD_ERR_FILE = "stderr.txt";

  private final Class launcherClass;
  private final List<String> commandListPrefix;
  private final String osCommandFormat;

  protected final ClasspathProvider classpathProvider;
  protected final REEFFileNames reefFileNames;

  AbstractCommandBuilder(
      final Class launcherClass,
      final List<String> commandListPrefix,
      final String osCommandFormat,
      final ClasspathProvider classpathProvider,
      final REEFFileNames reefFileNames) {
    this.launcherClass = launcherClass;
    this.commandListPrefix = commandListPrefix;
    this.osCommandFormat = osCommandFormat;

    this.classpathProvider = classpathProvider;
    this.reefFileNames = reefFileNames;
  }

  /**
   * Assembles the command to execute the Driver.
   */
  public String build(final JobSubmissionEvent jobSubmissionEvent) {
    return String.format(this.osCommandFormat, StringUtils.join(getCommandList(jobSubmissionEvent), ' '));
  }

  /**
   * Assembles the command to execute the Driver in list form.
   */
  private List<String> getCommandList(
      final JobSubmissionEvent jobSubmissionEvent) {

    return new JavaLaunchCommandBuilder(this.launcherClass, this.commandListPrefix)
        .setJavaPath("java")
        .setConfigurationFilePaths(Collections.singletonList(this.reefFileNames.getDriverConfigurationPath()))
        .setClassPath(getDriverClasspath())
        .setMemory(jobSubmissionEvent.getDriverMemory().get())
        .setStandardOut(STD_OUT_FILE)
        .setStandardErr(STD_ERR_FILE)
        .build();
  }

  /**
   * Returns the driver classpath string which is compatible with the intricacies of the OS.
   * @return classpath parameter string.
   */
  protected abstract String getDriverClasspath();
}
