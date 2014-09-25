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
package com.microsoft.reef.runtime.common.launch;

import com.microsoft.reef.runtime.common.Launcher;
import com.microsoft.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import com.microsoft.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import com.microsoft.reef.runtime.common.launch.parameters.LaunchID;
import com.microsoft.reef.util.EnvironmentUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class JavaLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final String DEFAULT_JAVA_PATH = System.getenv("JAVA_HOME") + "/bin/" + "java";
  private String stderrPath = null;
  private String stdoutPath = null;
  private String errorHandlerRID = null;
  private String launchID = null;
  private int megaBytes = 0;
  private String evaluatorConfigurationPath = null;
  private String javaPath = null;
  private String classPath = null;
  private Boolean assertionsEnabled = null;

  @Override
  public List<String> build() {
    return new ArrayList<String>() {{

      if (javaPath == null || javaPath.isEmpty()) {
        add(DEFAULT_JAVA_PATH);
      } else {
        add(javaPath);
      }

      add("-XX:PermSize=128m");
      add("-XX:MaxPermSize=128m");
      // Set Xmx based on am memory size
      add("-Xmx" + megaBytes + "m");

      if ((assertionsEnabled != null && assertionsEnabled)
          || EnvironmentUtils.areAssertionsEnabled()) {
        add("-ea");
      }

      if (classPath != null && !classPath.isEmpty()) {
        add("-classpath");
        add(classPath);
      }

      Launcher.propagateProperties(this, true, "proc_reef");
      Launcher.propagateProperties(this, false,
          "java.util.logging.config.file", "java.util.logging.config.class");

      add(Launcher.class.getName());

      add("-" + ErrorHandlerRID.SHORT_NAME);
      add(errorHandlerRID);
      add("-" + LaunchID.SHORT_NAME);
      add(launchID);
      add("-" + ClockConfigurationPath.SHORT_NAME);
      add(evaluatorConfigurationPath);

      if (stdoutPath != null && !stdoutPath.isEmpty()) {
        add("1>");
        add(stdoutPath);
      }

      if (stderrPath != null && !stderrPath.isEmpty()) {
        add("2>");
        add(stderrPath);
      }
    }};
  }

  @Override
  public JavaLaunchCommandBuilder setErrorHandlerRID(final String errorHandlerRID) {
    this.errorHandlerRID = errorHandlerRID;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setLaunchID(final String launchID) {
    this.launchID = launchID;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setConfigurationFileName(final String configurationFileName) {
    this.evaluatorConfigurationPath = configurationFileName;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setStandardOut(final String standardOut) {
    this.stdoutPath = standardOut;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setStandardErr(final String standardErr) {
    this.stderrPath = standardErr;
    return this;
  }

  /**
   * Set the path to the java executable. Will default to a heuristic search if not set.
   *
   * @param path
   * @return this
   */
  public JavaLaunchCommandBuilder setJavaPath(final String path) {
    this.javaPath = path;
    return this;
  }

  public JavaLaunchCommandBuilder setClassPath(final String classPath) {
    this.classPath = classPath;
    return this;
  }

  public JavaLaunchCommandBuilder setClassPath(final Collection<String> classPathElements) {
    this.classPath = "\"" + StringUtils.join(classPathElements, File.pathSeparatorChar) + "\"";
    return this;
  }

  /**
   * Enable or disable assertions on the child process.
   * If not set, the setting is taken from the JVM that executes the code.
   *
   * @param assertionsEnabled
   * @return this
   */
  public JavaLaunchCommandBuilder enableAssertions(final boolean assertionsEnabled) {
    this.assertionsEnabled = assertionsEnabled;
    return this;
  }
}
