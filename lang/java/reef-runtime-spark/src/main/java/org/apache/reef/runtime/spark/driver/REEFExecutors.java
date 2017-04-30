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
package org.apache.reef.runtime.mesos.driver;

import org.apache.reef.runtime.mesos.util.EvaluatorControl;
import org.apache.reef.runtime.mesos.util.EvaluatorLaunch;
import org.apache.reef.runtime.mesos.util.EvaluatorRelease;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Driver's view of SparkExecutors running in the cluster.
 */
final class REEFExecutors {
  private final Map<String, REEFExecutor> executors = new ConcurrentHashMap<>();

  @Inject
  REEFExecutors() {
  }

  public void add(final String id,
                  final int memory,
                  final EventHandler<EvaluatorControl> evaluatorControlHandler) {
    executors.put(id, new REEFExecutor(memory, evaluatorControlHandler));
  }

  public void remove(final String id) {
    this.executors.remove(id);
  }

  public Set<String> getExecutorIds() {
    return executors.keySet();
  }

  public int getMemory(final String id) {
    return executors.get(id).getMemory();
  }

  public void launchEvaluator(final EvaluatorLaunch evaluatorLaunch) {
    executors.get(evaluatorLaunch.getIdentifier().toString()).launchEvaluator(evaluatorLaunch);
  }

  public void releaseEvaluator(final EvaluatorRelease evaluatorRelease) {
    executors.get(evaluatorRelease.getIdentifier().toString()).releaseEvaluator(evaluatorRelease);
  }
}
