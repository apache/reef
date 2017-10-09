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
package org.apache.reef.runtime.spark.driver;

import org.apache.reef.runtime.spark.util.EvaluatorControl;
import org.apache.reef.runtime.spark.util.EvaluatorLaunch;
import org.apache.reef.runtime.spark.util.EvaluatorRelease;
import org.apache.reef.wake.EventHandler;

/**
 * The Driver's view of a REEFExecutor running in the cluster.
 */
final class REEFExecutor {
  private final int memory;
  private final EventHandler<EvaluatorControl> evaluatorControlHandler;

  REEFExecutor(final int memory,
               final EventHandler<EvaluatorControl> evaluatorControlHandler) {
    this.memory = memory;
    this.evaluatorControlHandler = evaluatorControlHandler;
  }

  public void launchEvaluator(final EvaluatorLaunch evaluatorLaunch) {
    this.evaluatorControlHandler.onNext(new EvaluatorControl(evaluatorLaunch, null));
  }

  public void releaseEvaluator(final EvaluatorRelease evaluatorRelease) {
    this.evaluatorControlHandler.onNext(new EvaluatorControl(null, evaluatorRelease));
  }

  public int getMemory() {
    return this.memory;
  }
}
