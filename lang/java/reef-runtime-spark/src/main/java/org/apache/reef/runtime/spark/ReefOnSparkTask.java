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
package org.apache.reef.runtime.spark;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.reef.task.Task;

class ReefOnSparkTask implements Task {
  private Logger logger = Logger.getLogger(ReefOnSparkTask.class.getName());
  //LOG.log(Level.FINE, "Instantiated ReefOnSparkTask");

  public byte[] call(final byte[] bytes) {
    logger.log(Level.INFO, "Hello Spark!");
    return null;
  }
}
