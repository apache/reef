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
package org.apache.reef.runtime.spark.job;


import org.apache.reef.client.DriverLauncher;
import org.apache.reef.tang.Configuration;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

/**
 * Responsible for running the REEF task inside the spark execution environment.
 */
public class SparkRunner {

  /**
   * Given an input dataframe run a map function over
   * the spark context after we partition the data.
   * We use a map function which contains a lambda to run
   * the DataLoader over all the spark partitions.  This means
   * that the spark containers will manage the reef tasks being underneath.
   * @param runtimeConfiguration a handle to the runtime that we're using (yarn or local)
   * @param dataLoadConfiguration a handle to the configuration associated with the dataload process
   * @param inputPath the directory where the file is located.
   * @param numPartitions the number of partitions to use
   */
  public void run(final Configuration runtimeConfiguration,
                  final Configuration dataLoadConfiguration,
                  final String inputPath, final int numPartitions) {
    RDD<String> lines;
    SparkSession sparkSession=null;
    sparkSession = SparkSession.builder().master("local").appName("ReefOnSpark").getOrCreate();
    if (inputPath!=null && !inputPath.isEmpty()){
      lines = sparkSession.sparkContext().textFile(inputPath, numPartitions);
      lines.map(status->DriverLauncher.getLauncher(runtimeConfiguration).run(dataLoadConfiguration));
    }
  }
}

