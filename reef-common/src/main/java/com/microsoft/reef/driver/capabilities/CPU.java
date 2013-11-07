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
package com.microsoft.reef.driver.capabilities;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * A {@link Capability} that represents the CPUs available to an Evaluator.
 */
@DriverSide
@Public
@Provided
public final class CPU implements Capability {

  private final int cores;

  /**
   * @param cores
   */
  public CPU(final int cores) {
    assert (cores > 0);
    this.cores = cores;
  }

  /**
   * Default toString()
   *
   * @return
   */
  @Override
  public String toString() {
    return "CPU [cores=" + this.cores + "]";
  }

  /**
   * @return the number of cores in the Evaluator
   */
  public final int getCores() {
    return this.cores;
  }
}
