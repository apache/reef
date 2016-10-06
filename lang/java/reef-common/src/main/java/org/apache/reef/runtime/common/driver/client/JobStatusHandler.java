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
package org.apache.reef.runtime.common.driver.client;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * Generic interface for job status messages' handler.
 * Receive JobStatusProto messages and keep the last message so it can be retrieved via getLastStatus() method.
 */
@DriverSide
@DefaultImplementation(RemoteClientJobStatusHandler.class)
public interface JobStatusHandler extends EventHandler<ReefServiceProtos.JobStatusProto> {

  /**
   * Return the last known status of the REEF job.
   * Can return null if the job has not been launched yet.
   * @return Last status of the REEF job.
   */
  ReefServiceProtos.JobStatusProto getLastStatus();
}
