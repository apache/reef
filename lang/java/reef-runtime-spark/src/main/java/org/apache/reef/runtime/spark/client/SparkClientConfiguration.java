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
package org.apache.reef.runtime.spark.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.spark.SparkClasspathProvider;
import org.apache.reef.runtime.spark.client.parameters.MasterIp;
import org.apache.reef.runtime.spark.client.parameters.RootFolder;
import org.apache.reef.runtime.spark.util.HDFSConfigurationConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A ConfigurationModule for the Mesos resource manager.
 */
@Public
@ClientSide
public class SparkClientConfiguration extends ConfigurationModuleBuilder {
  /**
   * The folder in which the sub-folders for REEF drivers, one per job, will be created.
   * If none is given, a folder "REEF_SPARK_RUNTIME" will be created in the local directory.
   */
  public static final OptionalParameter<String> ROOT_FOLDER = new OptionalParameter<>();

  /**
   * The ip address of Spark Master.
   */
  public static final RequiredParameter<String> MASTER_IP = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new SparkClientConfiguration()
      .merge(CommonRuntimeConfiguration.CONF)
      .bindImplementation(JobSubmissionHandler.class, SparkJobSubmissionHandler.class)
      .bindImplementation(DriverConfigurationProvider.class, SparkDriverConfigurationProviderImpl.class)
      .bindNamedParameter(RootFolder.class, ROOT_FOLDER)
      .bindNamedParameter(MasterIp.class, MASTER_IP)
      .bindConstructor(Configuration.class, HDFSConfigurationConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, SparkClasspathProvider.class)
      .build();
}
