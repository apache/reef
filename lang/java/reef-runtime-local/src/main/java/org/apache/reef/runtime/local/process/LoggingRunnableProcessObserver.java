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
package org.apache.reef.runtime.local.process;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A RunnableProcessObserver that logs the events/
 */
public final class LoggingRunnableProcessObserver implements RunnableProcessObserver {
  private static final Logger LOG = Logger.getLogger(LoggingRunnableProcessObserver.class.getName());

  @Override
  public void onProcessStarted(final String processId) {
    LOG.log(Level.FINE, "Process {0} started.", processId);

  }

  @Override
  public void onProcessExit(final String processId, final int exitCode) {
    if (exitCode == 0) {
      LOG.log(Level.FINE, "Process {0} exited with return code 0.", processId);
    } else {
      LOG.log(Level.WARNING, "Process {0} exited with return code {1}.", new Object[]{processId, exitCode});
    }
  }
}
