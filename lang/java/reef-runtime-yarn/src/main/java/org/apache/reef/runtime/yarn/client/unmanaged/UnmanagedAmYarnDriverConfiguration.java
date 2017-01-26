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
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.RemoteConfiguration;

/**
 * Build configuration for REEF driver running in unmanaged mode under the YARN resource manager.
 */
@Public
@DriverSide
public final class UnmanagedAmYarnDriverConfiguration {

  private static final Tang TANG = Tang.Factory.getTang();

  public static Configuration build(final String appId, final String driverRootPath) {

    final Configuration yarnDriverModule = YarnDriverConfiguration.CONF
        .set(YarnDriverConfiguration.JOB_IDENTIFIER, appId)
        .set(YarnDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
        .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, driverRootPath)
        .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
        .build();

    return TANG.newConfigurationBuilder(yarnDriverModule)
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_UNMANAGED_DRIVER")
        .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .build();
  }

  /** Cannot instantiate this utility class. */
  private UnmanagedAmYarnDriverConfiguration() { }
}
