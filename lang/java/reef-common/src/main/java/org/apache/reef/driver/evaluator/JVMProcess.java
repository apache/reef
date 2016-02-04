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
package org.apache.reef.driver.evaluator;

import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Defines the setup of a JVM process.
 * Users can set JVM options via {@link #setMemory(int)} and {@link #addOption(String)}.
 * Runtimes can also set JVM options, but should not do so if users have set options by
 * checking {@link #isOptionSet()}.
 */
public final class JVMProcess implements EvaluatorProcess {
  private final JavaLaunchCommandBuilder commandBuilder = new JavaLaunchCommandBuilder();
  private final RuntimePathProvider runtimePathProvider;
  private final ClasspathProvider classpathProvider;
  private boolean optionSet = false;

  /**
   * Instantiated via JVMProcessFactory.
   */
  JVMProcess(final RuntimePathProvider runtimePathProvider,
             final ClasspathProvider classpathProvider) {
    this.runtimePathProvider = runtimePathProvider;
    this.classpathProvider = classpathProvider;
  }

  @Override
  public List<String> getCommandLine() {
    return commandBuilder
        .setJavaPath(runtimePathProvider.getPath())
        .setClassPath(classpathProvider.getEvaluatorClasspath())
        .build();
  }

  @Override
  public EvaluatorType getType() {
    return EvaluatorType.JVM;
  }

  @Override
  public JVMProcess setMemory(final int megaBytes) {
    commandBuilder.setMemory(megaBytes);
    optionSet = true;
    return this;
  }

  @Override
  public boolean isOptionSet() {
    return optionSet;
  }

  @Override
  public JVMProcess setConfigurationFileName(final String configurationFileName) {
    commandBuilder.setConfigurationFilePaths(Collections.singletonList(configurationFileName));
    return this;
  }

  @Override
  public JVMProcess setStandardOut(final String standardOut) {
    commandBuilder.setStandardOut(standardOut);
    return this;
  }

  @Override
  public JVMProcess setStandardErr(final String standardErr) {
    commandBuilder.setStandardErr(standardErr);
    return this;
  }

  /**
   * Add a JVM option.
   * @param option The full option, e.g. "-XX:+PrintGCDetails", "-Xms500m"
   * @return this
   */
  public JVMProcess addOption(final String option) {
    commandBuilder.addOption(option);
    optionSet = true;
    return this;
  }
}
