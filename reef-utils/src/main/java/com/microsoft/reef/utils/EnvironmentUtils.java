/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.utils;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.Param;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class EnvironmentUtils {

  private static final Logger LOG = Logger.getLogger(EnvironmentUtils.class.getName());

  public static String getReefHome() {
    final String reefHome = System.getProperty("REEF_HOME", System.getenv("REEF_HOME"));
    assert (null != reefHome) : "REEF_HOME is not set";
    return reefHome;
  }

  public static String getReefVersion() {
    final String envVersion = System.getenv("REEF_VERSION");
    return System.getProperty("REEF_VERSION", envVersion == null ? "0.1-SNAPSHOT" : envVersion);
  }

  public static String getClassLocationFile(final Class<?> clazz) {
    return clazz.getProtectionDomain().getCodeSource().getLocation().getFile();
  }

  /**
   * Get a set of all classpath entries EXCEPT of those under
   * $JAVA_HOME, $YARN_HOME, and $HADOOP_HOME.
   *
   * @return A set of classpath entries as strings.
   */
  public static Set<String> getAllClasspathJars() {
    return getAllClasspathJars("JAVA_HOME", "YARN_HOME", "HADOOP_HOME");
  }

  /**
   * Get a set of all classpath entries EXCEPT of those under excludeEnv directories.
   * Every excludeEnv entry is an environment variable name.
   *
   * @return A set of classpath entries as strings.
   */
  public static Set<String> getAllClasspathJars(final String... excludeEnv) {

    final Set<String> jars = new HashSet<>();
    final Set<Path> excludePaths = new HashSet<>();

    for (final String env : excludeEnv) {
      final File file = new File(env);
      if (file.exists()) {
        excludePaths.add(file.toPath());
      }
    }

    for (final String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      try {
        final File file = new File(path);
        if (file.exists()) {
          final Path absolutePath = file.toPath();
          boolean toBeAdded = true;
          for (final Path prefix : excludePaths) {
            if (absolutePath.startsWith(prefix)) {
              toBeAdded = false;
            }
          }
          if (toBeAdded) {
            jars.add(absolutePath.toString());
          }
        }
      } catch (final InvalidPathException ex) {
        LOG.log(Level.FINE, "Skip path: {0}: {1}", new Object[] { path, ex });
      }
    }

    return jars;
  }

  public static <P extends Param> ConfigurationModule addAll(
      ConfigurationModule config, final P param, final Iterable<String> values) {
    for (final String val : values) {
      config = config.set(param, val);
    }
    return config;
  }

  public static ConfigurationModule addClasspath(
      ConfigurationModule config, final OptionalParameter<String> param) {
    return addAll(config, param, getAllClasspathJars());
  }
}
