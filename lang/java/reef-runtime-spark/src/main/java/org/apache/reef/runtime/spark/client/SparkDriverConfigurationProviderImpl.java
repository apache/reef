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

import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.spark.client.parameters.MasterIp;
import org.apache.reef.runtime.spark.driver.SparkDriverConfiguration;
import org.apache.reef.runtime.spark.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.net.URI;

/**
 * Default driver configuration provider for Spark.
 */
final class SparkDriverConfigurationProviderImpl implements DriverConfigurationProvider {

  private final String masterIp;
  private final double jvmSlack;

  @Inject
  SparkDriverConfigurationProviderImpl(@Parameter(MasterIp.class) final String masterIp,
                                       @Parameter(JVMHeapSlack.class) final double jvmSlack) {
    this.masterIp = masterIp;
    this.jvmSlack = jvmSlack;
  }

  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(SparkDriverConfiguration.CONF
                    .set(SparkDriverConfiguration.SPARK_MASTER_IP, this.masterIp)
                    .set(SparkDriverConfiguration.JOB_IDENTIFIER, jobId)
                    .set(SparkDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
                    .set(SparkDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
                    .set(SparkDriverConfiguration.SCHEDULER_DRIVER_CAPACITY, 1)
                    // must be 1 as there is 1 scheduler at the same time
                    .set(SparkDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
                    .build(),
            applicationConfiguration);
  }
}
