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
package com.microsoft.reef.runtime.common.files;

import java.util.List;

/**
 * Interface to be implemented by each REEF runtime (YARN, Mesos, Local) to provide additional classpath elements to be
 * postfixed to the user classpath.
 */
public interface RuntimeClasspathProvider {

  /**
   * @return the classpath to be appended to the Driver's classpath.
   */
  List<String> getDriverClasspath();

  /**
   * @return the classpath to be appended to every Evaluator's classpath.
   */
  List<String> getEvaluatorClasspath();

}
