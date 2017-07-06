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

import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.reef.runtime.spark.InputStreamReaderRunnable;

/**
 * This class submits a Spark job to a Spark Cluster
 * from a Java client
 * <p>
 * To accomplish submitting a Spark job from a Java
 * client, we use the SparkLauncher class.
 */
public class SparkJobSubmitter{

  static final Logger LOG=Logger.getLogger(SparkJobSubmitter.class);

  public void submitJob(final String[] arguments) throws Exception{
    long startTime=System.currentTimeMillis();
    submitJobHelper(arguments); // ... the code being measured ...
    long estimatedTime=System.currentTimeMillis()-startTime;
    LOG.info("estimatedTime (millis)="+estimatedTime);
  }

  public void submitJobHelper(final String[] arguments) throws Exception{
    final String javaHome=arguments[0];
    final String sparkHome=arguments[1];
    final String appResource=arguments[2];
    final String mainClass=arguments[3];
    //
    // parameters passed to the  actual spark job to be run
    final String[] appArgs=new String[]{"stubbedvaluefornow"};
    //
    //
    SparkLauncher spark=new SparkLauncher().setVerbose(true)
        .setJavaHome(javaHome).setSparkHome(sparkHome).setAppResource(appResource)    // "/my/app.jar"
        .setMainClass(mainClass)        // "my.spark.app.Main"
        .setMaster("local").setConf(SparkLauncher.DRIVER_MEMORY, "1g").addAppArgs(appArgs);
    //
    // Launches a sub-process that will start the configured Spark application.
    Process proc=spark.launch();
    InputStreamReaderRunnable inputStreamReaderRunnable=new InputStreamReaderRunnable(proc.getInputStream(), "input");
    Thread inputThread=new Thread(inputStreamReaderRunnable, "LogStreamReader input");
    inputThread.start();
    InputStreamReaderRunnable errorStreamReaderRunnable=new InputStreamReaderRunnable(proc.getErrorStream(), "error");
    Thread errorThread=new Thread(errorStreamReaderRunnable, "LogStreamReader error");
    errorThread.start();
    LOG.info("Waiting for finish...");
    int exitCode=proc.waitFor();
    LOG.info("Finished! Exit code:"+exitCode);
  }
}
