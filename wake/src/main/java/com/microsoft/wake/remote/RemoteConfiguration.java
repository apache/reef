/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

/**
 * Configuration options and helper methods for Wake remoting.
 */
public final class RemoteConfiguration {

  @NamedParameter(doc = "The name of the remote manager.")
  public static final class ManagerName implements Name<String> {
    // Intentionally empty
  }

  @NamedParameter(doc = "The host address to be used for messages.")
  public static final class HostAddress implements Name<String> {
    // Intentionally empty
  }

  @NamedParameter(doc = "The port to be used for messages.")
  public static final class Port implements Name<Integer> {
    // Intentionally empty
  }

  @NamedParameter(doc = "The codec to be used for messages.")
  public static final class MessageCodec implements Name<Codec<?>> {
    // Intentionally empty
  }

  @NamedParameter(doc = "The event handler to be used for throwables", default_class = DefaultErrorHandler.class)
  public static final class ErrorHandler implements Name<EventHandler<Throwable>> {
    // Intentionally empty
  }
  
  @NamedParameter(doc = "Whether or not to use the message ordering guarantee", default_value="true")
  public static final class OrderingGuarantee implements Name<Boolean> {
    // Intentionally empty
  }
}
