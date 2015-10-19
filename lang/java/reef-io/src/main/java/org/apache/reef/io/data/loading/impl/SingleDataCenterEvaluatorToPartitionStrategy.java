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
package org.apache.reef.io.data.loading.impl;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.tang.annotations.Parameter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * This is an online version which satisfies
 * requests in a greedy way, for single data center network topologies.
 */
@DriverSide
public final class SingleDataCenterEvaluatorToPartitionStrategy extends AbstractEvaluatorToPartitionStrategy {
  private static final Logger LOG = Logger
      .getLogger(SingleDataCenterEvaluatorToPartitionStrategy.class.getName());

  @Inject
  SingleDataCenterEvaluatorToPartitionStrategy(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(DistributedDataSetPartitionSerializer.DistributedDataSetPartitions.class)
      final Set<String> serializedDataPartitions) {
    super(inputFormatClassName, serializedDataPartitions);
  }

  @Override
  protected void updateLocations(final NumberedSplit<InputSplit> numberedSplit) {
    try {
      final InputSplit split = numberedSplit.getEntry();
      final String[] locations = split.getLocations();
      for (final String location : locations) {
        BlockingQueue<NumberedSplit<InputSplit>> newSplitQue = new LinkedBlockingQueue<>();
        final BlockingQueue<NumberedSplit<InputSplit>> splitQue = locationToSplits.putIfAbsent(location, newSplitQue);
        if (splitQue != null) {
          newSplitQue = splitQue;
        }
        newSplitQue.add(numberedSplit);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  @Override
  protected NumberedSplit<InputSplit> tryAllocate(final NodeDescriptor nodeDescriptor, final String evaluatorId) {
    LOG.fine("Picking a random split from the unallocated ones");
    return allocateSplit(evaluatorId, unallocatedSplits);
  }

}
