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

import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.wake.EventHandler;

import java.util.logging.Logger;

public class TopologyFailedEvaluatorHandler implements EventHandler<FailedEvaluator> {

  private static final Logger LOG = Logger.getLogger(TopologyFailedEvaluatorHandler.class.getName());


  private final CommunicationGroupDriverImpl communicationGroupDriverImpl;

  public TopologyFailedEvaluatorHandler(final CommunicationGroupDriverImpl communicationGroupDriverImpl) {
    this.communicationGroupDriverImpl = communicationGroupDriverImpl;
  }

  @Override
  public void onNext(final FailedEvaluator failedEvaluator) {
    final String failedEvaluatorId = failedEvaluator.getId();
    LOG.entering("TopologyFailedEvaluatorHandler", "onNext", failedEvaluatorId);
    if (failedEvaluator.getFailedTask().isPresent()) {
      final String failedTaskId = failedEvaluator.getFailedTask().get().getId();
      LOG.finest("Failed Evaluator contains a failed task: " + failedTaskId);
      communicationGroupDriverImpl.failTask(failedTaskId);
      communicationGroupDriverImpl.removeTask(failedTaskId);
    }
    LOG.exiting("TopologyFailedEvaluatorHandler", "onNext", failedEvaluatorId);
  }

}
