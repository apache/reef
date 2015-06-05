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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.wake.EventHandler;

import java.util.logging.Logger;

public class TopologyRunningTaskHandler implements EventHandler<RunningTask> {

  private static final Logger LOG = Logger.getLogger(TopologyRunningTaskHandler.class.getName());

  private final CommunicationGroupDriverImpl communicationGroupDriverImpl;

  public TopologyRunningTaskHandler(final CommunicationGroupDriverImpl communicationGroupDriverImpl) {
    this.communicationGroupDriverImpl = communicationGroupDriverImpl;
  }

  @Override
  public void onNext(final RunningTask runningTask) {
    final String runningTaskId = runningTask.getId();
    LOG.entering("TopologyRunningTaskHandler", "onNext", runningTaskId);
    communicationGroupDriverImpl.runTask(runningTaskId);
    LOG.exiting("TopologyRunningTaskHandler", "onNext", runningTaskId);
  }

}
