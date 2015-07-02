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
package org.apache.reef.tests.rack.awareness;

import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

/**
 * A Driver start handler that requests a single Evaluator of size 64MB in the
 * specified rack.
 */
public final class OnDriverStartedAllocateOneInRack implements EventHandler<StartTime> {

  private final EvaluatorRequestor requestor;
  private final String rackName;

  @Inject
  OnDriverStartedAllocateOneInRack(
      final EvaluatorRequestor requestor,
      @Parameter(RackNameParameter.class) final String rackName) {
    this.requestor = requestor;
    this.rackName = rackName;
  }

  @Override
  public void onNext(final StartTime startTime) {
    this.requestor.submit(EvaluatorRequest.newBuilder().setMemory(64).setNumber(1).setNumberOfCores(1)
        .addRackName(rackName).build());
  }
}
