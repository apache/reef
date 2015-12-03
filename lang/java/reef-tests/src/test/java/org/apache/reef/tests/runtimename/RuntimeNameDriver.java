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
package org.apache.reef.tests.runtimename;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.driver.RuntimeNameTestConfiguration;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class RuntimeNameDriver {
  private static final Logger LOG = Logger.getLogger(RuntimeNameDriver.class.getName());
  private final String runtimeName;

  @Inject
  RuntimeNameDriver(@Parameter(RuntimeNameTestConfiguration.RuntimeName.class) final String name) {
    runtimeName = name;
  }

  /**
   * Handles AllocatedEvaluator: Submit the HelloTask.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator allocated with runtime: {0}",
          allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName());
      LOG.log(Level.INFO, "Evaluator expected runtime runtime: {0}", RuntimeNameDriver.this.runtimeName);
      if (!RuntimeNameDriver.this.runtimeName.equals(
          allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName())) {
        throw new DriverSideFailure(
            "Got an Evaluator with different runtime name then expected. Expected "
                + RuntimeNameDriver.this.runtimeName
                + ", but got " + allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName());
      }

      allocatedEvaluator.close();
    }
  }
}