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
package org.apache.reef.runtime.common.launch;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.util.EnvironmentUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Build the launch command for Java REEF processes.
 */
public final class JavaLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final Logger LOG = Logger.getLogger(JavaLaunchCommandBuilder.class.getName());

  private static final String DEFAULT_JAVA_PATH = System.getenv("JAVA_HOME") + "/bin/" + "java";
  private static final String[] DEFAULT_OPTIONS = {"-XX:PermSize=128m", "-XX:MaxPermSize=128m"};
  private String stderrPath = null;
  private String stdoutPath = null;
  private String evaluatorConfigurationPath = null;
  private String javaPath = null;
  private String classPath = null;
  private Boolean assertionsEnabled = null;
  private final Map<String, JVMOption> options = new HashMap<>();
  private final List<String> commandPrefixList;
  private final Class launcherClass;

  /**
   * Constructor that populates default options, using the default Launcher
   * class {@link REEFLauncher}.
   */
  public JavaLaunchCommandBuilder() {
    this(REEFLauncher.class, null);
  }

  /**
   * Constructor that uses the default Launcher class, {@link REEFLauncher}.
   * @param commandPrefixList
   */
  public JavaLaunchCommandBuilder(final List<String> commandPrefixList) {
    this(REEFLauncher.class, commandPrefixList);
  }

  /**
   * Constructor that populates prefix and uses a custom Launcher class.
   */
  public JavaLaunchCommandBuilder(final Class launcherClass, final List<String> commandPrefixList) {
    for (final String defaultOption : DEFAULT_OPTIONS) {
      addOption(defaultOption);
    }

    this.launcherClass = launcherClass;
    this.commandPrefixList = commandPrefixList;
  }

  @Override
  public List<String> build() {
    return new ArrayList<String>() {{
        if (commandPrefixList != null) {
          for (final String cmd : commandPrefixList) {
            add(cmd);
          }
        }

        if (javaPath == null || javaPath.isEmpty()) {
          add(DEFAULT_JAVA_PATH);
        } else {
          add(javaPath);
        }

        if (assertionsEnabled != null && assertionsEnabled
            || EnvironmentUtils.areAssertionsEnabled()) {
          addOption("-ea");
        }

        for (final JVMOption jvmOption : options.values()) {
          add(jvmOption.toString());
        }

        if (classPath != null && !classPath.isEmpty()) {
          add("-classpath");
          add(classPath);
        }

        propagateProperties(this, true, "proc_reef");
        propagateProperties(this, false,
            "java.util.logging.config.file", "java.util.logging.config.class");

        add(launcherClass.getName());
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
  @SuppressWarnings("checkstyle:hiddenfield")
  public JavaLaunchCommandBuilder setMemory(final int megaBytes) {
    return addOption(JVMOption.parse("-Xmx" + megaBytes + "m"));
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
   * @param path Path to the java executable.
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
    this.classPath = StringUtils.join(classPathElements, File.pathSeparatorChar);
    return this;
  }

  /**
   * Add a JVM option.
   * @param option The full option, e.g. "-XX:+PrintGCDetails"
   * @return this
   */
  public JavaLaunchCommandBuilder addOption(final String option) {
    return addOption(JVMOption.parse(option));
  }

  /**
   * Pass values of the properties specified in the propNames array as <code>-D...</code>
   * command line parameters. Currently used only to pass logging configuration to child JVMs processes.
   *
   * @param vargs     List of command line parameters to append to.
   * @param copyNull  create an empty parameter if the property is missing in current process.
   * @param propNames property names.
   */
  private static void propagateProperties(
      final Collection<String> vargs, final boolean copyNull, final String... propNames) {
    for (final String propName : propNames) {
      final String propValue = System.getProperty(propName);
      if (propValue == null || propValue.isEmpty()) {
        if (copyNull) {
          vargs.add("-D" + propName);
        }
      } else {
        vargs.add(String.format("-D%s=%s", propName, propValue));
      }
    }
  }

  private JavaLaunchCommandBuilder addOption(final JVMOption jvmOption) {
    if (options.containsKey(jvmOption.option)) {
      LOG.warning("Replaced option " + options.get(jvmOption.option) + " with " + jvmOption);
    }
    options.put(jvmOption.option, jvmOption);
    return this;
  }

  /**
   * Enable or disable assertions on the child process.
   * If not set, the setting is taken from the JVM that executes the code.
   *
   * @param assertionsEnabled If true, enable assertions.
   * @return this
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public JavaLaunchCommandBuilder enableAssertions(final boolean assertionsEnabled) {
    this.assertionsEnabled = assertionsEnabled;
    return this;
  }

  /**
   * Represents the JVM option as a option and value, combined by a separator.
   * There are many different JVM option formats. This implementation only recognizes
   * equals-separated and -Xm[nsx] memory options. All other option formats are
   * represented with an option and empty value and separator.
   */
  static final class JVMOption {
    static final Pattern EQUALS = Pattern.compile("(.+)=(.+)");
    static final Pattern MEMORY = Pattern.compile("(\\-Xm[nsx])(.+)");

    public final String option;
    public final String value;
    public final String separator;

    private JVMOption(final String option, final String value,
                     final String separator) {
      this.option = option;
      this.value = value;
      this.separator = separator;
    }

    static JVMOption parse(final String string) {

      final String trimmed = string.trim();

      final Matcher equalsMatcher = EQUALS.matcher(trimmed);
      if (equalsMatcher.matches()) {
        return new JVMOption(equalsMatcher.group(1), equalsMatcher.group(2), "=");
      }

      final Matcher memoryMatcher = MEMORY.matcher(trimmed);
      if (memoryMatcher.matches()) {
        return new JVMOption(memoryMatcher.group(1), memoryMatcher.group(2), "");
      }

      // Unknown options return the entire string as the option
      return new JVMOption(trimmed, "", "");
    }

    public String toString() {
      return option + separator + value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final JVMOption jvmOption = (JVMOption) o;

      if (!option.equals(jvmOption.option)) {
        return false;
      }
      if (!value.equals(jvmOption.value)) {
        return false;
      }
      return separator.equals(jvmOption.separator);

    }

    @Override
    public int hashCode() {
      int result = option.hashCode();
      result = 31 * result + value.hashCode();
      result = 31 * result + separator.hashCode();
      return result;
    }
  }
}
