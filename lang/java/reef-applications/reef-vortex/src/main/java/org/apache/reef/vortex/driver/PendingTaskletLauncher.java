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
package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pending Tasklet Launcher.
 */
@DriverSide
final class PendingTaskletLauncher implements EventHandler<Integer> {
  private static final Logger LOG = Logger.getLogger(PendingTaskletLauncher.class.getName());

  private final RunningWorkers runningWorkers;
  private final PendingTasklets pendingTasklets;

  @Inject
  private PendingTaskletLauncher(final RunningWorkers runningWorkers,
                                 final PendingTasklets pendingTasklets) {
    this.runningWorkers = runningWorkers;
    this.pendingTasklets = pendingTasklets;
  }

  /**
   * Repeatedly take a tasklet from the pending queue and launch it via RunningWorkers.
   */
  @Override
  public void onNext(final Integer integer) {
    while (!runningWorkers.isTerminated()) {
      try {
        final Tasklet tasklet = pendingTasklets.takeFirst(); // blocks when no tasklet exists
        runningWorkers.launchTasklet(tasklet); // blocks when no worker exists
      } catch (InterruptedException e) {
        LOG.log(Level.INFO, "Interrupted upon termination");
      }
    }
  }
}
