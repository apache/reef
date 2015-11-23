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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

import java.util.List;

/**
 * Event from Driver Runtime to Driver Process.
 * A status update from the Driver Runtime to the Driver Process
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(RuntimeStatusEventImpl.class)
public interface RuntimeStatusEvent {
  /**
   * @return Name of the Runtime
   */
  String getName();

  /**
   * @return State of the Runtime
   */
  State getState();

  /**
   * @return List of allocated containers
   */
  List<String> getContainerAllocationList();

  /**
   * @return Error from the Runtime
   */
  Optional<ReefServiceProtos.RuntimeErrorProto> getError();

  /**
   * @return Number of outstanding container requests
   */
  Optional<Integer> getOutstandingContainerRequests();
}
