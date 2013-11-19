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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.common.client.defaults.*;
import com.microsoft.reef.util.RuntimeError;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

/**
 * Hosts all named parameters for REEF Client.
 */
@Private
@Provided
@ClientSide
public final class ClientConfigurationOptions {

  @NamedParameter(doc = "Client EventHandler that gets messages from the Driver.",
                  default_classes = DefaultJobMessageHandler.class)
  public final static class JobMessageHandler implements Name<EventHandler<JobMessage>> {
  }

  @NamedParameter(doc = "Client EventHandler triggered when the REEF job is running.",
                  default_classes = DefaultRunningJobHandler.class)
  public final static class RunningJobHandler implements Name<EventHandler<RunningJob>> {
  }

  @NamedParameter(doc = "Client EventHandler triggered when REEF job completes.",
                  default_classes = DefaultCompletedJobHandler.class)
  public final static class CompletedJobHandler implements Name<EventHandler<CompletedJob>> {
  }

  @NamedParameter(doc = "Client EventHandler triggered on remote job failure.",
                  default_classes = DefaultFailedJobHandler.class)
  public final static class FailedJobHandler implements Name<EventHandler<FailedJob>> {
  }

  @NamedParameter(doc = "Client EventHandler triggered on REEF runtime error.",
                  default_classes = DefaultRuntimeErrorHandler.class)
  public final static class RuntimeErrorHandler implements Name<EventHandler<RuntimeError>> {
  }
}
