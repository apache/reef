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
package org.apache.reef.runtime.azbatch.util.command;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.evaluator.EvaluatorShimLauncher;
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimePathProvider;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Build the launch command for Java REEF processes for Azure Batch Linux pools.
 */
@Private
public class LinuxCommandBuilder extends AbstractCommandBuilder {

  private static final Class LAUNCHER_CLASS = REEFLauncher.class;
  private static final Class SHIM_LAUNCHER_CLASS = EvaluatorShimLauncher.class;
  private static final List<String> COMMAND_LIST_PREFIX =
      Collections.unmodifiableList(Arrays.asList(
          "unzip " + AzureBatchFileNames.getTaskJarFileName() + " -d 'reef/'" + ";"));
  private static final char CLASSPATH_SEPARATOR_CHAR = ':';
  private static final String OS_COMMAND_FORMAT = "/bin/sh -c \"%s\"";

  @Inject
  LinuxCommandBuilder(
      final ClasspathProvider classpathProvider,
      final RuntimePathProvider runtimePathProvider,
      final REEFFileNames reefFileNames,
      final AzureBatchFileNames azureBatchFileNames) {
    super(LAUNCHER_CLASS, SHIM_LAUNCHER_CLASS, COMMAND_LIST_PREFIX, OS_COMMAND_FORMAT,
        classpathProvider, runtimePathProvider, reefFileNames, azureBatchFileNames);
  }

  @Override
  protected String getDriverClasspath() {
    return StringUtils.join(super.classpathProvider.getDriverClasspath(), CLASSPATH_SEPARATOR_CHAR);
  }

  @Override
  protected String getEvaluatorShimClasspath() {
    return StringUtils.join(super.classpathProvider.getEvaluatorClasspath(), CLASSPATH_SEPARATOR_CHAR);
  }

  @Override
  public String getIpAddressFilePath() {
    return "$AZ_BATCH_JOB_PREP_WORKING_DIR/hostip.txt";
  }

  @Override
  public String captureIpAddressCommandLine() {
    final String filePath = getIpAddressFilePath();
    return String.format("/bin/bash -c \"rm -f %s; echo `hostname -i` > %s\"", filePath, filePath);
  }
}
