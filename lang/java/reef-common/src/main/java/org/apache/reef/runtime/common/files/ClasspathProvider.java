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
package org.apache.reef.runtime.common.files;

import net.jcip.annotations.Immutable;
import org.apache.reef.annotations.audience.RuntimeAuthor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Supplies the classpath to REEF for process (Driver, Evaluator) launches.
 */
@Immutable
@RuntimeAuthor
public final class ClasspathProvider {
  private final List<String> driverClasspath;
  private final List<String> evaluatorClasspath;


  @Inject
  ClasspathProvider(final RuntimeClasspathProvider runtimeClasspathProvider,
                    final REEFFileNames reefFileNames) {
    final List<String> baseClasspath = Arrays.asList(
        reefFileNames.getLocalFolderPath() + "/*",
        reefFileNames.getGlobalFolderPath() + "/*");

    // Assemble the driver classpath
    final List<String> runtimeDriverClasspathPrefix = runtimeClasspathProvider.getDriverClasspathPrefix();
    final List<String> runtimeDriverClasspathSuffix = runtimeClasspathProvider.getDriverClasspathSuffix();
    final List<String> driverClasspath = new ArrayList<>(baseClasspath.size() +
        runtimeDriverClasspathPrefix.size() +
        runtimeDriverClasspathSuffix.size());
    driverClasspath.addAll(runtimeDriverClasspathPrefix);
    driverClasspath.addAll(baseClasspath);
    driverClasspath.addAll(runtimeDriverClasspathSuffix);
    this.driverClasspath = Collections.unmodifiableList(driverClasspath);

    // Assemble the evaluator classpath
    final List<String> runtimeEvaluatorClasspathPrefix = runtimeClasspathProvider.getEvaluatorClasspathPrefix();
    final List<String> runtimeEvaluatorClasspathSuffix = runtimeClasspathProvider.getEvaluatorClasspathSuffix();
    final List<String> evaluatorClasspath = new ArrayList<>(runtimeEvaluatorClasspathPrefix.size() +
        baseClasspath.size() +
        runtimeEvaluatorClasspathSuffix.size());
    evaluatorClasspath.addAll(runtimeEvaluatorClasspathPrefix);
    evaluatorClasspath.addAll(baseClasspath);
    evaluatorClasspath.addAll(runtimeEvaluatorClasspathSuffix);
    this.evaluatorClasspath = Collections.unmodifiableList(evaluatorClasspath);
  }

  /**
   * @return the classpath to be used for the Driver
   */
  public List<String> getDriverClasspath() {
    return this.driverClasspath;
  }

  /**
   * @return the classpath to be used for Evaluators.
   */
  public List<String> getEvaluatorClasspath() {
    return this.evaluatorClasspath;
  }
}
