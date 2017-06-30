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
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnDriverConfiguration;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

// Run:
// ..\spark\bin\spark-submit.cmd
//     --master yarn --deploy-mode cluster
//     --class org.apache.reef.examples.hellospark.ReefOnSpark
//     .\target\reef-examples-spark-0.16.0-SNAPSHOT-shaded.jar

public final class ReefOnSpark {

  private Logger LOG = Logger.getLogger(this.getClass.getName);

  private String rootFolder = ".";

  private UnmanagedAmYarnClientConfiguration runtimeConfig = UnmanagedAmYarnClientConfiguration.CONF
    .set(UnmanagedAmYarnClientConfiguration.ROOT_FOLDER, rootFolder)
    .build();

  public void process(String[] args) {

    LOG.setLevel(Level.FINEST);

    SparkConf conf = new SparkConf().setAppName("ReefOnSpark:host");
    SparkContext sc = new SparkContext(conf);

    try (DriverLauncher.getLauncher(runtimeConfig)) {

      String jarPath = EnvironmentUtils.getClassLocation(ReefOnSparkDriver.class);

      DriverConfiguration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "ReefOnSpark:hello")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, jarPath)
        .set(DriverConfiguration.ON_DRIVER_STARTED, classOf[ReefOnSparkDriver#StartHandler])
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, classOf[ReefOnSparkDriver#EvaluatorAllocatedHandler])
        .build();

      Integer appId = client.submit(driverConfig, 120000);

      //LOG.log(Level.INFO, "Job submitted: {0} to {1}", Array[AnyRef](appId, jarPath));


      UnmanagedAmYarnDriverConfiguration yarnAmConfig = UnmanagedAmYarnDriverConfiguration.CONF
        .set(UnmanagedAmYarnDriverConfiguration.JOB_IDENTIFIER, appId)
        .set(UnmanagedAmYarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, rootFolder)
        .build();

      try (reef <- managed(REEFEnvironment.fromConfiguration(client.getUser, yarnAmConfig, driverConfig))) {
        reef.run();
        //val status = reef.getLastStatus;
        //LOG.log(Level.INFO, "REEF job {0} completed: state {1}", Array[AnyRef](appId, status.getState))
      }
    }

    sc.stop();
  }
}
