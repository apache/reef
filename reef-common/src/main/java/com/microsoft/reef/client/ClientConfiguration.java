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

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalImpl;
import com.microsoft.tang.formats.RequiredImpl;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteConfiguration;

/**
 * A ConfigurationModule to fill out for the client configuration.
 */
public class ClientConfiguration extends ConfigurationModuleBuilder {

  /**
   * An implementation of JobObserver to inform of Job start and such.
   */
  public static final RequiredImpl<JobObserver> JOB_OBSERVER = new RequiredImpl<>();
  /**
   * An implementation of RuntimeErrorHandler to inform of runtime errors.
   * By default, a runtime error throws a RuntimeException in the client JVM.
   */
  public static final OptionalImpl<RuntimeErrorHandler> RUNTIME_ERROR_HANDLER = new OptionalImpl<>();
  /**
   * Error handler for events on Wake-spawned threads.
   * Exceptions that are thrown on wake-spawned threads (e.g. in EventHandlers) will be caught by Wake and delivered to
   * this handler. Default behvior is to log the exceptions and rethrow them as RuntimeExceptions.
   */
  public static final OptionalImpl<EventHandler<Throwable>> WAKE_ERROR_HANDLER = new OptionalImpl<>();

  public static final ConfigurationModule CONF = new ClientConfiguration()
      .bindImplementation(JobObserver.class, JOB_OBSERVER)
      .bindImplementation(RuntimeErrorHandler.class, RUNTIME_ERROR_HANDLER)
      .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, WAKE_ERROR_HANDLER)
      .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_CLIENT")
      .build();
}
