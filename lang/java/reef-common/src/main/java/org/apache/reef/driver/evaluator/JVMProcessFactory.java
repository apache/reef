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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.RuntimePathProvider;

import javax.inject.Inject;

/**
 * Factory to setup new JVM processes.
 */
@DriverSide
public final class JVMProcessFactory implements EvaluatorProcessFactory<JVMProcess> {
  private final RuntimePathProvider pathProvider;
  private final ClasspathProvider classpathProvider;

  @Inject
  private JVMProcessFactory(final RuntimePathProvider pathProvider,
                           final ClasspathProvider classpathProvider) {
    this.pathProvider = pathProvider;
    this.classpathProvider = classpathProvider;
  }

  @Override
  public JVMProcess newEvaluatorProcess() {
    return new JVMProcess(pathProvider, classpathProvider);
  }
}
