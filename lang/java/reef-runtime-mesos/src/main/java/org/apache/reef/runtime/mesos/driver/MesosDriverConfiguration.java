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
import org.apache.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.mesos.MesosClasspathProvider;
import org.apache.reef.runtime.mesos.driver.parameters.MesosMasterIp;
import org.apache.reef.runtime.mesos.util.HDFSConfigurationConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.StageConfiguration;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.time.Clock;

/**
 * Binds Driver's runtime event handlers.
 */
public final class MesosDriverConfiguration extends ConfigurationModuleBuilder {
  /**
   * @see AbstractDriverRuntimeConfiguration.JobIdentifier.class
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see AbstractDriverRuntimeConfiguration.EvaluatorTimeout
   */
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();

  /**
   * The ip address of Mesos Master.
   */
  public static final RequiredParameter<String> MESOS_MASTER_IP = new RequiredParameter<>();

  /**
   * The client remote identifier.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * Capacity for runnning Mesos Scheduler Driver.
   */
  public static final RequiredParameter<Integer> SCHEDULER_DRIVER_CAPACITY = new RequiredParameter<>();

  public static ConfigurationModule CONF = new MesosDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, MesosResourceLaunchHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, MesosResourceReleaseHandler.class)
      .bindImplementation(ResourceRequestHandler.class, MesosResourceRequestHandler.class)
      .bindSetEntry(Clock.RuntimeStartHandler.class, MesosRuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, MesosRuntimeStopHandler.class)
      .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)

      .bindNamedParameter(MesosMasterIp.class, MESOS_MASTER_IP)
      .bindConstructor(Configuration.class, HDFSConfigurationConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, MesosClasspathProvider.class)

      .bindNamedParameter(StageConfiguration.Capacity.class, SCHEDULER_DRIVER_CAPACITY)
      .bindNamedParameter(StageConfiguration.StageHandler.class, MesosSchedulerDriverExecutor.class)
      .bindImplementation(EStage.class, SingleThreadStage.class)

          // Bind the fields bound in AbstractDriverRuntimeConfiguration
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .build();
}
