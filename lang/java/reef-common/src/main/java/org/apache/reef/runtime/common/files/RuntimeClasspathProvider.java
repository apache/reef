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

import java.util.List;

/**
 * Interface to be implemented by each REEF runtime (YARN, Mesos, Local) to provide additional classpath elements to be
 * pre- and postfixed to the user classpath.
 */
public interface RuntimeClasspathProvider {

  /**
   * @return the classpath to be prefixed in front of the user classpath of the Driver.
   */
  List<String> getDriverClasspathPrefix();

  /**
   * @return the classpath to be suffixed after of the user classpath of the Driver.
   */
  List<String> getDriverClasspathSuffix();

  /**
   * @return the classpath to be prefixed in front of the user classpath of each Evaluator.
   */
  List<String> getEvaluatorClasspathPrefix();

  /**
   * @return the classpath to be suffixed after of the user classpath of each Evaluator.
   */
  List<String> getEvaluatorClasspathSuffix();
}
