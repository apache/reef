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
package org.apache.reef.javabridge;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.javabridge.avro.DefinedRuntimes;
import org.apache.reef.javabridge.utils.DefinedRuntimesSerializer;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.evaluator.EvaluatorRequestor}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "EvaluatorRequestorClr2Java.cpp" },
    CsFiles = { "IEvaluatorRequestorClr2Java.cs", "EvaluatorRequestor.cs" })
public final class EvaluatorRequestorBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(EvaluatorRequestorBridge.class.getName());
  private final boolean isBlocked;
  private final EvaluatorRequestor jevaluatorRequestor;
  private final LoggingScopeFactory loggingScopeFactory;
  private final Set<String> definedRuntimes;

  // accumulate how many evaluators have been submitted through this instance
  // of EvaluatorRequestorBridge
  private int clrEvaluatorsNumber;

  public EvaluatorRequestorBridge(final EvaluatorRequestor evaluatorRequestor,
                                  final boolean isBlocked,
                                  final LoggingScopeFactory loggingScopeFactory,
                                  final Set<String> definedRuntimes) {
    this.jevaluatorRequestor = evaluatorRequestor;
    this.clrEvaluatorsNumber = 0;
    this.isBlocked = isBlocked;
    this.loggingScopeFactory = loggingScopeFactory;
    this.definedRuntimes = definedRuntimes;
  }

  public void submit(final int evaluatorsNumber,
                     final int memory,
                     final int virtualCore,
                     final String rack,
                     final String runtimeName) {
    if (this.isBlocked) {
      throw new RuntimeException("Cannot request additional Evaluator, this is probably because " +
          "the Driver has crashed and restarted, and cannot ask for new container due to YARN-2433.");
    }

    if (rack != null && !rack.isEmpty()) {
      LOG.log(Level.WARNING, "Ignoring rack preference.");
    }

    try (final LoggingScope ls = loggingScopeFactory.evaluatorRequestSubmitToJavaDriver(evaluatorsNumber)) {
      clrEvaluatorsNumber += evaluatorsNumber;

      final EvaluatorRequest request = EvaluatorRequest.newBuilder()
          .setNumber(evaluatorsNumber)
          .setMemory(memory)
          .setNumberOfCores(virtualCore)
          .setRuntimeName(runtimeName)
          .build();

      LOG.log(Level.FINE, "submitting evaluator request {0}", request);
      jevaluatorRequestor.submit(request);
    }
  }

  public int getEvaluatorNumber() {
    return clrEvaluatorsNumber;
  }

  @Override
  public void close() {
  }

  public byte[] getDefinedRuntimes(){
    if(LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "Defined Runtimes :" + StringUtils.join(this.definedRuntimes, ','));
    }

    final DefinedRuntimes dr = new DefinedRuntimes();
    final List<CharSequence> runtimeNames = new ArrayList<>();
    for(final String name : this.definedRuntimes) {
      runtimeNames.add(name);
    }
    dr.setRuntimeNames(runtimeNames);
    final DefinedRuntimesSerializer drs = new DefinedRuntimesSerializer();
    final byte[] ret = drs.toBytes(dr);
    return ret;
  }
}
