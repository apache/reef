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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.Iterator;


/**
 * Responsible for running the REEF task inside the spark execution environment.
 */
public class SparkRunner implements Serializable {


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
    JavaRDD<String> lines;
    SparkSession sparkSession=null;
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("LineCounter");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    if (inputPath!=null && !inputPath.isEmpty()){
      lines = sc.textFile(inputPath, numPartitions);
      //lines.map(status->DriverLauncher.getLauncher(runtimeConfiguration).run(dataLoadConfiguration));

      JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(final String s) throws Exception {
              DriverLauncher.getLauncher(runtimeConfiguration).run(dataLoadConfiguration);
              return null;
            }
          });
    }
  }
}


