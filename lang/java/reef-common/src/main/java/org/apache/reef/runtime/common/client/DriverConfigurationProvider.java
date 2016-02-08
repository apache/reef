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
package org.apache.reef.runtime.common.client;

import org.apache.reef.tang.Configuration;

import java.net.URI;

/**
 * A contract for driver configuration provider.
 */
public interface DriverConfigurationProvider {
  /**
   * Generates driver configuration.
   * @param jobFolder The job folder.
   * @param clientRemoteId the client remote id.
   * @param jobId The job id.
   * @param applicationConfiguration The application configuration.
   * @return Generated driver configuration.
   */
  Configuration getDriverConfiguration(
          final URI jobFolder,
          final String clientRemoteId,
          final String jobId,
          final Configuration applicationConfiguration);
}
