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
package org.apache.reef.runtime.spark;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnDriverConfiguration;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.spark.SparkConf;
import org.apache.reef.tang.Configuration;
import org.apache.spark.SparkContext;

// Run:
// ..\spark\bin\spark-submit.cmd
//     --master yarn --deploy-mode cluster
//     --class org.apache.reef.examples.hellospark.ReefOnSpark
//     .\target\reef-examples-spark-0.16.0-SNAPSHOT-shaded.jar


/**
 * Abstraction around submitting a reef job on spark.
 */
public final class ReefOnSpark {

  private Logger logger = Logger.getLogger(ReefOnSpark.class.getName());

  private static final String ROOTFOLDER = ".";

  private static final Configuration RUNTIME_CONFIG = UnmanagedAmYarnClientConfiguration.CONF
      .set(UnmanagedAmYarnClientConfiguration.ROOT_FOLDER, ROOTFOLDER)
      .build();

  public void process(final String[] args) throws InjectionException {

    logger.setLevel(Level.FINEST);

    SparkConf conf = new SparkConf().setAppName("ReefOnSpark:host");
    SparkContext sc = new SparkContext(conf);

    try (final DriverLauncher client = DriverLauncher.getLauncher(RUNTIME_CONFIG)) {
      String jarPath = EnvironmentUtils.getClassLocation(ReefOnSpark.class);
      Configuration driverConfig = DriverConfiguration.CONF
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "ReefOnSpark:hello")
          .set(DriverConfiguration.GLOBAL_LIBRARIES, jarPath)
          .set(DriverConfiguration.ON_DRIVER_STARTED, ReefOnSparkDriver.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ReefOnSparkDriver.EvaluatorAllocatedHandler.class)
          .build();
      final String appId = client.submit(driverConfig, 120000);
      //LOG.log(Level.INFO, "Job submitted: {0} to {1}", Array[AnyRef](appId, jarPath));
      final Configuration yarnAmConfig = UnmanagedAmYarnDriverConfiguration.CONF
          .set(UnmanagedAmYarnDriverConfiguration.JOB_IDENTIFIER, appId)
          .set(UnmanagedAmYarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, ROOTFOLDER)
          .build();
      try (REEFEnvironment reef = REEFEnvironment.fromConfiguration(client.getUser(), yarnAmConfig, driverConfig)) {
        reef.run();
        final ReefServiceProtos.JobStatusProto status = reef.getLastStatus();
        logger.log(Level.INFO, "REEF job {0} completed: state {1}", new Object[]{appId, status.getState()});
      }
    }
    sc.stop();
  }
}
