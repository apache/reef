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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;

import javax.inject.Inject;

/**
 * Driver Client evaluator requestor.
 */
public final class DriverClientEvaluatorRequestor implements EvaluatorRequestor {

  private final IDriverServiceClient driverServiceClient;

  @Inject
  private DriverClientEvaluatorRequestor(final IDriverServiceClient driverServiceClient) {
    this.driverServiceClient = driverServiceClient;
  }

  @Override
  public void submit(final EvaluatorRequest req) {
    this.driverServiceClient.onEvaluatorRequest(req);
  }

  @Override
  public EvaluatorRequest.Builder newRequest() {
    return new DriverClientEvaluatorRequestor.Builder();
  }

  /**
   * {@link DriverClientEvaluatorRequestor.Builder} extended with a new submit method.
   * {@link EvaluatorRequest}s are built using this builder.
   */
  public final class Builder extends EvaluatorRequest.Builder<DriverClientEvaluatorRequestor.Builder> {
    @Override
    public synchronized void submit() {
      DriverClientEvaluatorRequestor.this.submit(this.build());
    }
  }
}
