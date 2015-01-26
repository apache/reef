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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.proto.DriverRuntimeProtocol;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
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
    LOG.log(Level.FINEST, "Got an EvaluatorRequest: number: {0}, memory = {1}, cores = {2}.", new Object[]{req.getNumber(), req.getMegaBytes(), req.getNumberOfCores()});

    if (req.getMegaBytes() <= 0) {
      throw new IllegalArgumentException("Given an unsupported memory size: " + req.getMegaBytes());
    }
    if (req.getNumberOfCores() <= 0) {
      throw new IllegalArgumentException("Given an unsupported core number: " + req.getNumberOfCores());
    }
    if (req.getNumber() <= 0) {
      throw new IllegalArgumentException("Given an unsupported number of evaluators: " + req.getNumber());
    }

    try (LoggingScope ls = loggingScopeFactory.evaluatorSubmit(req.getNumber())) {
      final DriverRuntimeProtocol.ResourceRequestProto.Builder request = DriverRuntimeProtocol.ResourceRequestProto
          .newBuilder()
          .setResourceCount(req.getNumber())
          .setVirtualCores(req.getNumberOfCores())
          .setMemorySize(req.getMegaBytes());

      final ResourceCatalog.Descriptor descriptor = req.getDescriptor();
      if (descriptor != null) {
        if (descriptor instanceof RackDescriptor) {
          request.addRackName(descriptor.getName());
        } else if (descriptor instanceof NodeDescriptor) {
          request.addNodeName(descriptor.getName());
        } else {
          throw new IllegalArgumentException("Unable to operate on descriptors of type " + descriptor.getClass().getName());
        }
      }

      this.resourceRequestHandler.onNext(request.build());
    }
  }
}
