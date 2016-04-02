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
package org.apache.reef.runtime.mesos.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.WorkingDirectoryTempFileCreator;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.driver.parameters.DefinedRuntimes;
import org.apache.reef.runtime.common.driver.parameters.EvaluatorTimeout;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.mesos.MesosClasspathProvider;
import org.apache.reef.runtime.mesos.driver.parameters.MesosMasterIp;
import org.apache.reef.runtime.mesos.driver.parameters.MesosSlavePort;
import org.apache.reef.runtime.mesos.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.runtime.mesos.util.HDFSConfigurationConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.StageConfiguration;
import org.apache.reef.wake.impl.SingleThreadStage;

/**
 * Binds Driver's runtime event handlers.
 */
public final class MesosDriverConfiguration extends ConfigurationModuleBuilder {
  /**
   * @see JobIdentifier
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see EvaluatorTimeout
   */
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();

  /**
   * The ip address of Mesos Master.
   */
  public static final RequiredParameter<String> MESOS_MASTER_IP = new RequiredParameter<>();

  /**
   * The name of the configured runtimes.
   */
  public static final RequiredParameter<String> RUNTIME_NAMES = new RequiredParameter<>();
  /**
   * The port number of Mesos Slave.
   */
  public static final OptionalParameter<Integer> MESOS_SLAVE_PORT = new OptionalParameter<>();

  /**
   * The client remote identifier.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * Capacity for running Mesos Scheduler Driver.
   */
  public static final RequiredParameter<Integer> SCHEDULER_DRIVER_CAPACITY = new RequiredParameter<>();

  /**
   * The client remote identifier.
   */
  public static final OptionalParameter<String> JOB_SUBMISSION_DIRECTORY_PREFIX = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new MesosDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, MesosResourceLaunchHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, MesosResourceReleaseHandler.class)
      .bindImplementation(ResourceRequestHandler.class, MesosResourceRequestHandler.class)
      .bindImplementation(ResourceManagerStartHandler.class, MesosRuntimeStartHandler.class)
      .bindImplementation(ResourceManagerStopHandler.class, MesosRuntimeStopHandler.class)
      .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)

      .bindNamedParameter(MesosMasterIp.class, MESOS_MASTER_IP)
      .bindNamedParameter(MesosSlavePort.class, MESOS_SLAVE_PORT)
      .bindConstructor(Configuration.class, HDFSConfigurationConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, MesosClasspathProvider.class)

      .bindNamedParameter(StageConfiguration.Capacity.class, SCHEDULER_DRIVER_CAPACITY)
      .bindNamedParameter(JobSubmissionDirectoryPrefix.class, JOB_SUBMISSION_DIRECTORY_PREFIX)
      .bindNamedParameter(StageConfiguration.StageHandler.class, MesosSchedulerDriverExecutor.class)
      .bindImplementation(EStage.class, SingleThreadStage.class)

      // Bind the fields bound in AbstractDriverRuntimeConfiguration
      .bindNamedParameter(JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(LaunchID.class, JOB_IDENTIFIER)
      .bindNamedParameter(EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
      .bindNamedParameter(ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(ErrorHandlerRID.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindSetEntry(DefinedRuntimes.class, RUNTIME_NAMES)
      .build();
}
