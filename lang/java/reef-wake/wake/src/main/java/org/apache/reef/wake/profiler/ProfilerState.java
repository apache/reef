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
package org.apache.reef.wake.profiler;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * A class that contains parameters and states for wake profiler.
 */
public final class ProfilerState {

  private ProfilerState() {}

  /**
   * Parameter to enable Wake network profiling. By default profiling is disabled.
   */
  @NamedParameter(doc = "If true, profiling will be enabled", short_name = "profiling", default_value = "false")
  private static final class ProfilingEnabled implements Name<Boolean> { }

  /**
   * Gets the class of the NamedParameter ProfilingEnabled.
   *
   * @return Class of ProflingEnabled which should be Name<Boolean>
   */
  private static Class<? extends Name<Boolean>> getProfilingEnabledClass() {
    return ProfilingEnabled.class;
  }

  /**
   * Checks if profiling is enabled.
   *
   * @param injector the tang injector that stores value of ProfilingEnabled.
   * @return true if profiling is enabled
   * @throws InjectionException if name resolution fails
   */
  public static boolean isProfilingEnabled(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(getProfilingEnabledClass());
  }
}
