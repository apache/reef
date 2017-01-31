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
package org.apache.reef.client;

import org.apache.reef.client.parameters.*;
import org.apache.reef.runtime.common.client.parameters.ClientPresent;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteConfiguration;

/**
 * A ConfigurationModule to fill out for the client configuration.
 */
public final class ClientConfiguration extends ConfigurationModuleBuilder {

  /**
   * Event handler for messages from the running job.
   * Default implementation just writes message to the log.
   * A message contains a status and a client-defined message payload.
   */
  public static final OptionalImpl<EventHandler<JobMessage>> ON_JOB_MESSAGE = new OptionalImpl<>();

  /**
   * Handler for the event when a REEF job is submitted to the Resource Manager.
   * Default implementation just writes the new job ID to the log.
   */
  public static final OptionalImpl<EventHandler<SubmittedJob>> ON_JOB_SUBMITTED = new OptionalImpl<>();

  /**
   * Handler for the event when a submitted REEF Job is running.
   * Default implementation just writes to the log.
   */
  public static final OptionalImpl<EventHandler<RunningJob>> ON_JOB_RUNNING = new OptionalImpl<>();

  /**
   * Handler for the event when a submitted REEF Job is completed.
   * Default implementation just writes to the log.
   */
  public static final OptionalImpl<EventHandler<CompletedJob>> ON_JOB_COMPLETED = new OptionalImpl<>();

  /**
   * Handler for the event when a submitted REEF Job has failed.
   * Default implementation logs an error and rethrows the exception in the client JVM.
   */
  public static final OptionalImpl<EventHandler<FailedJob>> ON_JOB_FAILED = new OptionalImpl<>();

  /**
   * Receives fatal resourcemanager errors. The presence of this error means that the
   * underlying REEF instance is no longer able to execute REEF jobs. The
   * actual Jobs may or may not still be running.
   * Default implementation logs an error and rethrows the exception in the client JVM.
   */
  public static final OptionalImpl<EventHandler<FailedRuntime>> ON_RUNTIME_ERROR = new OptionalImpl<>();

  /**
   * Error handler for events on Wake-spawned threads.
   * Exceptions that are thrown on wake-spawned threads (e.g. in EventHandlers) will be caught by Wake and delivered to
   * this handler. Default behavior is to log the exceptions and rethrow them as RuntimeExceptions.
   */
  public static final OptionalImpl<EventHandler<Throwable>> ON_WAKE_ERROR = new OptionalImpl<>();

  public static final ConfigurationModule CONF = new ClientConfiguration()
      .bind(JobMessageHandler.class, ON_JOB_MESSAGE)
      .bind(JobSubmittedHandler.class, ON_JOB_SUBMITTED)
      .bind(JobRunningHandler.class, ON_JOB_RUNNING)
      .bind(JobCompletedHandler.class, ON_JOB_COMPLETED)
      .bind(JobFailedHandler.class, ON_JOB_FAILED)
      .bind(ResourceManagerErrorHandler.class, ON_RUNTIME_ERROR)
      .bindNamedParameter(ClientPresent.class, ClientPresent.YES)
      .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, ON_WAKE_ERROR)
      .build();
}
