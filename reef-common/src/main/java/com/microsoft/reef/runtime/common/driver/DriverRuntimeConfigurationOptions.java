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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

/**
 * Houses the Driver resourcemanager configuration's NamedParameters
 */
public class DriverRuntimeConfigurationOptions {
  @NamedParameter(doc = "Called when a job control message is received by the client.")
  public final static class JobControlHandler implements Name<EventHandler<ClientRuntimeProtocol.JobControlProto>> {
  }
}
