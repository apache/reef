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
package org.apache.reef.wake.remote.address;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Uses Tang to create the default LocalAddressProvider.
 *
 * @deprecated Have an instance of LocalAddressProvider injected instead.
 */
@Deprecated
public final class LocalAddressProviderFactory {
  private static final Logger LOGGER = Logger.getLogger(LocalAddressProviderFactory.class.getName());
  private static LocalAddressProvider instance = null;

  /**
   * This class shall never be instantiated.
   */
  private LocalAddressProviderFactory() {
    // Intentionally left blank.
  }

  /**
   * @return the default LocalAddressProvider
   * @deprecated Have an instance of LocalAddressProvider injected instead.
   */
  public static LocalAddressProvider getInstance() {
    if (null == instance) {
      makeInstance();
    }
    return instance;
  }

  /**
   * Makes the instance.
   */
  private static void makeInstance() {
    assert (null == instance);
    try {
      LOGGER.log(Level.FINER, "Instantiating default LocalAddressProvider for legacy users.");
      instance = Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);
      LOGGER.log(Level.FINER, "Instantiated default LocalAddressProvider for legacy users.");
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to instantiate default LocalAddressProvider for legacy users.", e);
    }
    assert (null != instance);
  }
}
