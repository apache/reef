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
package org.apache.reef.io.data.loading.api;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.io.data.loading.impl.NumberedSplit;

/**
 * Interface that tracks the mapping between evaluators and the data partitions
 * assigned to those evaluators. Its part of the implementation of a
 * {@link org.apache.reef.io.data.loading.api.DataLoadingService} that uses the
 * Hadoop {@link org.apache.hadoop.mapred.InputFormat} to partition the data and
 * request resources accordingly
 *
 * @param <V>
 */
@DriverSide
@Unstable
public interface EvaluatorToPartitionStrategy<V extends InputSplit> {

  /**
   * Returns an input split for the given evaluator.
   * @param nodeDescriptor
   *      the node descriptor where the evaluator is running on
   * @param evalId
   *      the evaluator id
   * @return
   *      the numberedSplit
   * @throws RuntimeException if no split could be allocated
   */
  NumberedSplit<V> getInputSplit(NodeDescriptor nodeDescriptor, String evalId);

  /**
   * Returns the total number of splits computed in this strategy.
   * @return
   *  the number of splits
   */
  int getNumberOfSplits();

}
