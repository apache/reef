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
 * A {@link Capability} that captures the memory configuration of an Evaluator.
 */
@DriverSide
@Public
@Provided
public final class RAM implements Capability {

  private final int megaBytes;

  /**
   * @param megaBytes the amount of RAM represented in MegaBytes (2^20 Bytes)
   */
  public RAM(final int megaBytes) {
    this.megaBytes = megaBytes;
  }

  /**
   * @return
   */
  @Override
  public String toString() {
    return "RAM(megaBytes=" + this.megaBytes + ")";
  }

  /**
   * Access the amount of RAM represented in MegaBytes (2^20 Bytes)
   *
   * @return the amount of RAM represented in MegaBytes (2^20 Bytes)
   */
  public final int getMegaBytes() {
    return this.megaBytes;
  }
}
