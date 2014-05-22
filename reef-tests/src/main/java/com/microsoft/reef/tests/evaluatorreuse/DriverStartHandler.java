/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.evaluatorreuse;

import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;

final class DriverStartHandler implements EventHandler<StartTime> {
  private final EvaluatorRequestor requestor;

  @Inject
  DriverStartHandler(EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  @Override
  public void onNext(StartTime startTime) {
    this.requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(256)
        .build());
  }
}
