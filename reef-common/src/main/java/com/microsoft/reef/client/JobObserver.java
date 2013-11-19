/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * Implementations of this class will receive events related to a REEF Job.
 * <p/>
 * This class needs to be implemented by users of REEF.
 */
@Public
@ClientSide
@Deprecated
public interface JobObserver {

  /**
   * A message from the running job containing status and a client defined
   * message payload.
   *
   * @param message
   */
  public void onNext(final JobMessage message);

  /**
   * This method is called when a submitted REEF Job is running.
   *
   * @param job the running REEF job.
   */
  public void onNext(final RunningJob job);

  /**
   * Passes the completed job to the client.
   *
   * @param job completed object
   */
  public void onNext(final CompletedJob job);

  /**
   * @param job that failed including the JobException
   */
  public void onError(final FailedJob job);
}
