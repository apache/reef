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
package com.microsoft.reef.activity;

import com.microsoft.reef.annotations.audience.ActivitySide;
import com.microsoft.reef.annotations.audience.Public;

import java.util.concurrent.Callable;

/**
 * The interface for Activities.
 * <p/>
 * This interface is to be implemented for Activities.
 * <p/>
 * The main entry point for an Activity is the call() method inherited from
 * {@link Callable}. The REEF Evaluator will call this method in order to run
 * the Activity. The byte[] returned by it will be pushed to the Job Driver.
 */
@ActivitySide
@Public
public interface Activity {

  /**
   * Called by the runtime harness to execute the activity.
   *
   * @param memento the memento objected passed down by the driver.
   * @return the user defined return value
   * @throws Exception whenever the Activity encounters an unsolved issue.
   *                   This Exception will be thrown at the Driver's event handler.
   */
  public byte[] call(final byte[] memento) throws Exception;

}
