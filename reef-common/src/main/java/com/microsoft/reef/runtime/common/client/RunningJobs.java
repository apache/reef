/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.remote.RemoteMessage;

/**
 * Manages the RunningJobs a client knows about
 */
@Private
@ClientSide
@DefaultImplementation(RunningJobsImpl.class)
interface RunningJobs {

  /**
   * Closes all registered jobs forcefully.
   */
  public void closeAllJobs();

  /**
   * Processes a status message from a Job. If the Job is already known, the message will be passed on. If it is a
   * first message, a new RunningJob instance will be created for it.
   *
   * @param message
   */
  public void onJobStatusMessage(final RemoteMessage<ReefServiceProtos.JobStatusProto> message);

  /**
   * Processes a error message from the resource manager.
   *
   * @param runtimeFailure
   */
  public void onRuntimeErrorMessage(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> runtimeFailure);

}
