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
package org.apache.reef.runtime.local;

import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * RuntimeClasspathProvider for the local runtime.
 * <p/>
 * The prefix for the local runtime is empty, the suffix is the classpath of the current JVM. That classpath is filtered
 * to not contain subfolders of JAVA_HOME. Also, duplicates are removed.
 */
public final class LocalClasspathProvider implements RuntimeClasspathProvider {
  private final List<String> classPathSuffix;

  @Inject
  LocalClasspathProvider() {
    this.classPathSuffix = Collections.unmodifiableList(new ArrayList<>(getFilteredClasspath()));
  }

  /**
   * @return the classpath filtered by entries in subfolders of JAVA_HOME are removed, so are duplicate entries.
   */
  private static LinkedHashSet<String> getFilteredClasspath() {
    final LinkedHashSet<String> result = new LinkedHashSet<>();
    final Optional<Path> javaHome = getJavaHome();

    if (javaHome.isPresent()) {
      final Path javaHomePath = javaHome.get();
      for (final Path classPathEntry : getClasspath()) {
        if (!classPathEntry.startsWith(javaHomePath)) {
          result.add(toAbsolutePathString(classPathEntry));
        }
      }
    } else {
      for (final Path classPathEntry : getClasspath()) {
        result.add(toAbsolutePathString(classPathEntry));
      }
    }
    return result;
  }

  /**
   * @return the path to "JAVA_HOME", if that is set. Optional.empty(), else.
   */
  private static Optional<Path> getJavaHome() {
    final Optional<String> javaHome = getEnv("JAVA_HOME");

    if (javaHome.isPresent()) {
      final File javaHomeFile = new File(javaHome.get());
      if (javaHomeFile.exists()) {
        return Optional.of(javaHomeFile.toPath());
      }
    }
    return Optional.empty();
  }

  /**
   * @param envName
   * @return the value of the environment variable, if there is one.
   */
  private static Optional<String> getEnv(final String envName) {
    return Optional.ofNullable(System.getenv(envName));
  }

  /**
   * @return the classpath of the current JVM. Duplicates are removed.
   */
  private static LinkedHashSet<Path> getClasspath() {
    final LinkedHashSet<Path> result = new LinkedHashSet<>();
    for (final String classPathEntry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      final File file = new File(classPathEntry);
      if (file.exists()) {
        result.add(file.toPath());
      }
    }
    return result;
  }

  /**
   * Concerts the given path into a String representing the absolute path.
   *
   * @param path
   * @return
   */
  private static String toAbsolutePathString(final Path path) {
    return path.toAbsolutePath().toString();
  }

  @Override
  public List<String> getDriverClasspathPrefix() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getDriverClasspathSuffix() {
    return this.classPathSuffix;
  }

  @Override
  public List<String> getEvaluatorClasspathPrefix() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getEvaluatorClasspathSuffix() {
    return this.classPathSuffix;
  }
}
