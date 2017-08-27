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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEventImpl;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.common.utils.Constants;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Implementation of the EvaluatorRequestor that translates the request and hands it down to the underlying RM.
 */
public final class EvaluatorRequestorImpl implements EvaluatorRequestor {

  private static final Logger LOG = Logger.getLogger(EvaluatorRequestorImpl.class.getName());

  private final ResourceCatalog resourceCatalog;
  private final ResourceRequestHandler resourceRequestHandler;
  private final LoggingScopeFactory loggingScopeFactory;

  /**
   * @param resourceCatalog
   * @param resourceRequestHandler
   * @param loggingScopeFactory
   */
  @Inject
  public EvaluatorRequestorImpl(final ResourceCatalog resourceCatalog,
                                final ResourceRequestHandler resourceRequestHandler,
                                final LoggingScopeFactory loggingScopeFactory) {
    this.resourceCatalog = resourceCatalog;
    this.resourceRequestHandler = resourceRequestHandler;
    this.loggingScopeFactory = loggingScopeFactory;
  }

  @Override
  public synchronized void submit(final EvaluatorRequest req) {
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Got an EvaluatorRequest: number: {0}, memory = {1}, cores = {2}.",
          new Object[] {req.getNumber(), req.getMegaBytes(), req.getNumberOfCores()});
      LOG.log(Level.FINEST, "Node names: " + Arrays.toString(req.getNodeNames().toArray()));
    }

    if (req.getMegaBytes() <= 0) {
      throw new IllegalArgumentException("Given an unsupported memory size: " + req.getMegaBytes());
    }
    if (req.getNumberOfCores() <= 0) {
      throw new IllegalArgumentException("Given an unsupported core number: " + req.getNumberOfCores());
    }
    if (req.getNumber() <= 0) {
      throw new IllegalArgumentException("Given an unsupported number of evaluators: " + req.getNumber());
    }
    if (req.getNodeNames() == null) {
      throw new IllegalArgumentException("Node names cannot be null");
    }
    if (req.getRackNames() == null) {
      throw new IllegalArgumentException("Rack names cannot be null");
    }
    if(req.getRuntimeName() == null) {
      throw new IllegalArgumentException("Runtime name cannot be null");
    }
    // for backwards compatibility, we will always set the relax locality flag
    // to true unless the user has set it to false in the request, in which case
    // we will check for the ANY modifier (*), if there, then we relax the
    // locality regardless of the value set in the request.
    boolean relaxLocality = req.getRelaxLocality();
    if (!req.getRackNames().isEmpty()) {
      for (final String rackName : req.getRackNames()) {
        if (Constants.ANY_RACK.equals(rackName)) {
          relaxLocality = true;
          break;
        }
      }
    }

    try (LoggingScope ls = this.loggingScopeFactory.evaluatorSubmit(req.getNumber())) {
      final ResourceRequestEvent request = ResourceRequestEventImpl
          .newBuilder()
          .setResourceCount(req.getNumber())
          .setVirtualCores(req.getNumberOfCores())
          .setMemorySize(req.getMegaBytes())
          .addNodeNames(req.getNodeNames())
          .addRackNames(req.getRackNames())
          .setRelaxLocality(relaxLocality)
          .setRuntimeName(req.getRuntimeName())
          .setNodeLabels(req.getNodeLabels())
          .build();
      this.resourceRequestHandler.onNext(request);
    }
  }

  /**
   * Get a new builder.
   *
   * @return a new EvaluatorRequest Builder extended with the new submit method.
   */
  @Override
  public Builder newRequest() {
    return new Builder();
  }

  /**
   * {@link EvaluatorRequest.Builder} extended with a new submit method.
   * {@link EvaluatorRequest}s are built using this builder.
   */
  public final class Builder extends EvaluatorRequest.Builder<Builder> {
    public synchronized void submit() {
      EvaluatorRequestorImpl.this.submit(this.build());
    }
  }
}
