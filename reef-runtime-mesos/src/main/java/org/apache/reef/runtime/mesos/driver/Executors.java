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
package org.apache.reef.runtime.mesos.driver;

import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.EvaluatorLaunchProto;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.EvaluatorReleaseProto;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Driver's view of MesosExecutors running in the cluster.
 */
final class Executors {
  private final Map<String, Executor> executors = new ConcurrentHashMap<>();
  private final double jvmHeapFactor;

  @Inject
  Executors(final @Parameter(JVMHeapSlack.class) double jvmHeapSlack) {
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
  }

  public void add(final String id,
                  final int memory,
                  final EventHandler<EvaluatorLaunchProto> evaluatorLaunchHandler,
                  final EventHandler<EvaluatorReleaseProto> evaluatorReleaseHandler) {
    executors.put(id, new Executor(memory, evaluatorLaunchHandler, evaluatorReleaseHandler));
  }

  public void remove(final String id) {
    this.executors.remove(id);
  }

  public Set<String> getExecutorIds() { return executors.keySet(); }

  public int getEvaluatorMemory(final String id) {
    return (int) (this.jvmHeapFactor * executors.get(id).memory);
  }

  public void launchEvaluator(final String id,
                              final EvaluatorLaunchProto evaluatorLaunchProto) {
    executors.get(id).evaluatorLaunchHandler.onNext(evaluatorLaunchProto);
  }

  public void releaseEvaluator(final String id,
                               final EvaluatorReleaseProto evaluatorReleaseProto) {
    executors.get(id).evaluatorReleaseHandler.onNext(evaluatorReleaseProto);
  }

  final class Executor {
    private final int memory;
    private final EventHandler<EvaluatorLaunchProto> evaluatorLaunchHandler;
    private final EventHandler<EvaluatorReleaseProto> evaluatorReleaseHandler;

    Executor(final int memory,
             final EventHandler<EvaluatorLaunchProto> evaluatorLaunchHandler,
             final EventHandler<EvaluatorReleaseProto> evaluatorReleaseHandler) {
      this.memory = memory;
      this.evaluatorLaunchHandler = evaluatorLaunchHandler;
      this.evaluatorReleaseHandler = evaluatorReleaseHandler;
    }
  }
}