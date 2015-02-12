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
package org.apache.reef.io.network.nggroup.impl.driver;

import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.wake.EventHandler;

import java.util.logging.Logger;

public class TopologyFailedTaskHandler implements EventHandler<FailedTask> {

  private static final Logger LOG = Logger.getLogger(TopologyFailedTaskHandler.class.getName());


  private final CommunicationGroupDriverImpl communicationGroupDriverImpl;

  public TopologyFailedTaskHandler(final CommunicationGroupDriverImpl communicationGroupDriverImpl) {
    this.communicationGroupDriverImpl = communicationGroupDriverImpl;
  }

  @Override
  public void onNext(final FailedTask failedTask) {
    final String failedTaskId = failedTask.getId();
    LOG.entering("TopologyFailedTaskHandler", "onNext", failedTaskId);
    communicationGroupDriverImpl.failTask(failedTaskId);
    LOG.exiting("TopologyFailedTaskHandler", "onNext", failedTaskId);
  }

}
