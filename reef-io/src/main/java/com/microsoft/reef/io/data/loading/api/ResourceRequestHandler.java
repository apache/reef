/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.data.loading.api;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.wake.EventHandler;

/**
 * 
 */
public class ResourceRequestHandler implements EventHandler<EvaluatorRequest>{
  private static final Logger LOG = Logger.getLogger(ResourceRequestHandler.class.getName());
  private final EvaluatorRequestor requestor;
  private CountDownLatch resourceRequestGate = new CountDownLatch(1);

  public ResourceRequestHandler(EvaluatorRequestor requestor) {
    super();
    this.requestor = requestor;
  }

  public void releaseResourceRequestGate(){
    LOG.log(Level.FINE,"Releasing Gate");
    resourceRequestGate.countDown();
  }

  @Override
  public void onNext(EvaluatorRequest request) {
    try {
      LOG.log(Level.FINE,"Processing a request with count: " + request.getNumber());
      LOG.log(Level.FINE,"Waiting for gate to be released");
      resourceRequestGate.await();
      LOG.log(Level.FINE,"Gate released. Submitting request");
      LOG.log(Level.FINE,"First resetting latch");
      resourceRequestGate = new CountDownLatch(1);
      requestor.submit(request);
      LOG.log(Level.FINE,"End of request processing");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for compute requests to be satisfied", e);
    }
  }

}