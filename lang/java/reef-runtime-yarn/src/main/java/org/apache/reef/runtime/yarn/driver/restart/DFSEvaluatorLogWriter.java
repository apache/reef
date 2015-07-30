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
package org.apache.reef.runtime.yarn.driver.restart;

import org.apache.reef.annotations.audience.Private;

import java.io.IOException;

/**
 * The Evaluator log writer that writes to DFS. Currently supports regular append and append by overwrite.
 * Actual log entries should be immutable and no entry should ever be deleted. To remove an evaluator, a
 * removal entry should be preferred.
 */
@Private
public interface DFSEvaluatorLogWriter {

  /**
   * Writes a formatted entry (addition or removal) for an Evaluator ID into the DFS evaluator log.
   * @param formattedEntry The formatted entry (entry with evaluator ID and addition/removal information)
   * @throws IOException
   */
  void writeToEvaluatorLog(final String formattedEntry) throws IOException;
}
