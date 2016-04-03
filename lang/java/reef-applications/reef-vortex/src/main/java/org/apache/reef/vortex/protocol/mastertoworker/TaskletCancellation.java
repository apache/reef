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
package org.apache.reef.vortex.protocol.mastertoworker;

import org.apache.reef.annotations.Unstable;

/**
 * A {@link MasterToWorker} to cancel tasklets.
 */
@Unstable
public final class TaskletCancellation implements MasterToWorker {
  private int taskletId;

  /**
   * No-arg constructor required for Kryo to ser/des.
   */
  TaskletCancellation() {
  }

  public TaskletCancellation(final int taskletId) {
    this.taskletId = taskletId;
  }

  /**
   * @return the ID of the VortexTasklet associated with this MasterToWorker.
   */
  public int getTaskletId() {
    return taskletId;
  }

  @Override
  public Type getType() {
    return Type.CancelTasklet;
  }
}
