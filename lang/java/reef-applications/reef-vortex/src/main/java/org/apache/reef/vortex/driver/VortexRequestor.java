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
package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.vortex.common.KryoUtils;
import org.apache.reef.vortex.protocol.mastertoworker.MasterToWorkerRequest;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Takes the serialization work from the scheduler thread.
 */
@DriverSide
class VortexRequestor {
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final KryoUtils kryoUtils;

  @Inject
  VortexRequestor(final KryoUtils kryoUtils) {
    this.kryoUtils = kryoUtils;
  }

  /**
   * Sends a {@link MasterToWorkerRequest} asynchronously to a {@link org.apache.reef.vortex.evaluator.VortexWorker}.
   */
  void sendAsync(final RunningTask reefTask, final MasterToWorkerRequest masterToWorkerRequest) {
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        //  Possible race condition with VortexWorkerManager#terminate is addressed by the global lock in VortexMaster
        send(reefTask, masterToWorkerRequest);
      }
    });
  }

  /**
   * Sends a {@link MasterToWorkerRequest} synchronously to a {@link org.apache.reef.vortex.evaluator.VortexWorker}.
   */
  void send(final RunningTask reefTask, final MasterToWorkerRequest masterToWorkerRequest) {
    reefTask.send(kryoUtils.serialize(masterToWorkerRequest));
  }
}
