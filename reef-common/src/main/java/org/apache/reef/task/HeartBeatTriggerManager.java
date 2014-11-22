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
package org.apache.reef.task;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.runtime.common.evaluator.HeartBeatManager;

import javax.inject.Inject;

/**
 * Helper class for immediately sending heartbeat messages.
 * This can be used together with TaskMessageSource to push urgent messages to the Driver.
 * <p/>
 * CAUTION: Do not overuse as the Driver can be saturated with heartbeats.
 *
 * @see https://issues.apache.org/jira/browse/REEF-33 for the ongoing discussion of alternatives to this design.
 */
@TaskSide
@Public
@Unstable
public class HeartBeatTriggerManager {
  private final HeartBeatManager heartBeatManager;

  @Inject
  HeartBeatTriggerManager(final HeartBeatManager heartBeatManager) {
    this.heartBeatManager = heartBeatManager;
  }

  /**
   * Immediately send a heartbeat message to the Driver.
   */
  public void triggerHeartBeat() {
    this.heartBeatManager.sendHeartbeat();
  }
}