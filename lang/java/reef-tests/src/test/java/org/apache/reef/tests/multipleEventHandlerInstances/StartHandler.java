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
package org.apache.reef.tests.multipleEventHandlerInstances;

import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The start handler.
 */
public final class StartHandler implements EventHandler<StartTime> {

  private static final Logger LOG = Logger.getLogger(StartHandler.class.getName());
  private static int countInstances = 0;
  private final EvaluatorRequestor requestor;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public StartHandler(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    ++countInstances;
    if (countInstances > 1) {
      throw new DriverSideFailure("Expect StartHandler be created only once");
    }
  }

  @Override
  public void onNext(final StartTime startTime) {
    LOG.log(Level.INFO, "StartTime: ", startTime);
    StartHandler.this.requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(5)
        .setMemory(64)
        .setNumberOfCores(1)
        .build());
  }
}
