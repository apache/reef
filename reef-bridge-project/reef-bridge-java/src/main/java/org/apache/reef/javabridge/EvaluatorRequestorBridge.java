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
package org.apache.reef.javabridge;

import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.proto.DriverRuntimeProtocol;
import org.apache.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class EvaluatorRequestorBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(EvaluatorRequestorBridge.class.getName());
  private static final String REQUEST_CONTAINERS_ON_RACK = "*"; // The rack used for all requests
  private static final String REQUEST_CONTAINER_ON_HOST = "HostName"; // The hostname used for all requests.
  private final boolean isBlocked;
  private final EvaluatorRequestor jevaluatorRequestor;
  private final LoggingScopeFactory loggingScopeFactry;

  // accumulate how many evaluators have been submitted through this instance
  // of EvaluatorRequestorBridge
  private int clrEvaluatorsNumber;

  public EvaluatorRequestorBridge(final EvaluatorRequestor evaluatorRequestor, final boolean isBlocked, final LoggingScopeFactory loggingScopeFactry) {
    this.jevaluatorRequestor = evaluatorRequestor;
    this.clrEvaluatorsNumber = 0;
    this.isBlocked = isBlocked;
    this.loggingScopeFactry = loggingScopeFactry;
  }

  public void submit(final int evaluatorsNumber, final int memory, final int virtualCore, final String rack) {
    if (this.isBlocked) {
      throw new RuntimeException("Cannot request additional Evaluator, this is probably because the Driver has crashed and restarted, and cannot ask for new container due to YARN-2433.");
    }

    if (rack != null && !rack.isEmpty()) {
      LOG.log(Level.WARNING, "Ignoring rack request and using [{0}] instead.", REQUEST_CONTAINERS_ON_RACK);
    }

    try (final LoggingScope ls = loggingScopeFactry.evaluatorRequestSubmitToJavaDriver(evaluatorsNumber)) {
      clrEvaluatorsNumber += evaluatorsNumber;

      final ResourceCatalogImpl catalog = getNewResourceCatalogInstance();

      catalog.handle(DriverRuntimeProtocol.NodeDescriptorProto.newBuilder()
          .setRackName(REQUEST_CONTAINERS_ON_RACK)
          .setHostName(REQUEST_CONTAINER_ON_HOST)
          .setPort(0)
          .setMemorySize(memory)
          .setIdentifier("clrBridgeRackCatalog")
          .build());
      final RackDescriptor rackDescriptor = (RackDescriptor) (catalog.getRacks().toArray())[0];
      final EvaluatorRequest request = EvaluatorRequest.newBuilder().fromDescriptor(rackDescriptor)
          .setNumber(evaluatorsNumber)
          .setMemory(memory)
          .setNumberOfCores(virtualCore)
          .build();

      LOG.log(Level.FINE, "submitting evaluator request {0}", request);
      jevaluatorRequestor.submit(request);
    }
  }

  public int getEvaluatorNumber() {
    return clrEvaluatorsNumber;
  }

  /**
   * Helper method that uses Tang to generate a new instance of ResourceCatalogImpl
   *
   * @return a new instance of ResourceCatalogImpl
   */
  private ResourceCatalogImpl getNewResourceCatalogInstance() {
    try {
      return Tang.Factory.getTang().newInjector().getInstance(ResourceCatalogImpl.class);
    } catch (final InjectionException e) {
      LOG.log(Level.SEVERE, "Cannot inject resource catalog", e);
      throw new RuntimeException("Cannot inject resource catalog");
    }
  }

  @Override
  public void close() {
  }
}