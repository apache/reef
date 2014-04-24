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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.util.Optional;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import javax.inject.Inject;

/**
 * Helper class that holds on to the AM registration.
 */
final class ApplicationMasterRegistration {

  private Optional<RegisterApplicationMasterResponse> registration = Optional.empty();

  @Inject
  ApplicationMasterRegistration() {
  }

  /**
   * Set the registration information. This is a set-once field.
   *
   * @param registration
   */
  synchronized void setRegistration(final RegisterApplicationMasterResponse registration) {
    if (this.isPresent()) {
      throw new RuntimeException("Trying to re-register the AM");
    }
    this.registration = Optional.of(registration);
  }

  /**
   * @return the registered registration.
   */
  synchronized RegisterApplicationMasterResponse getRegistration() {
    return registration.get();
  }

  /**
   * @return true, if a registration was set.
   */
  synchronized boolean isPresent() {
    return this.registration.isPresent();
  }
}
