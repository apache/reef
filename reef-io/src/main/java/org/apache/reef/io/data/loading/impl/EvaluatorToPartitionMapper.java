/**
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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.data.loading.api.DataLoadingService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that tracks the mapping between
 * evaluators & the data partition assigned
 * to those evaluators. Its part of the
 * implementation of a {@link DataLoadingService}
 * that uses the Hadoop {@link InputFormat} to
 * partition the data and request resources
 * accordingly
 * <p/>
 * This is an online version which satisfies
 * requests in a greedy way.
 *
 * @param <V>
 */
@DriverSide
public class EvaluatorToPartitionMapper<V extends InputSplit> {
  private static final Logger LOG = Logger
      .getLogger(EvaluatorToPartitionMapper.class.getName());

  private final ConcurrentMap<String, BlockingQueue<NumberedSplit<V>>> locationToSplits = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, NumberedSplit<V>> evaluatorToSplits = new ConcurrentHashMap<>();
  private final BlockingQueue<NumberedSplit<V>> unallocatedSplits = new LinkedBlockingQueue<>();

  /**
   * Initializes the locations of splits mapping
   *
   * @param splits
   */
  public EvaluatorToPartitionMapper(V[] splits) {
    try {
      for (int splitNum = 0; splitNum < splits.length; splitNum++) {
        LOG.log(Level.FINE, "Processing split: " + splitNum);
        final V split = splits[splitNum];
        final String[] locations = split.getLocations();
        final NumberedSplit<V> numberedSplit = new NumberedSplit<V>(split, splitNum);
        unallocatedSplits.add(numberedSplit);
        for (final String location : locations) {
          BlockingQueue<NumberedSplit<V>> newSplitQue = new LinkedBlockingQueue<NumberedSplit<V>>();
          final BlockingQueue<NumberedSplit<V>> splitQue = locationToSplits.putIfAbsent(location,
              newSplitQue);
          if (splitQue != null) {
            newSplitQue = splitQue;
          }
          newSplitQue.add(numberedSplit);
        }
      }
      for (Map.Entry<String, BlockingQueue<NumberedSplit<V>>> locSplit : locationToSplits.entrySet()) {
        LOG.log(Level.FINE, locSplit.getKey() + ": " + locSplit.getValue().toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  /**
   * Get an input split to be assigned to this
   * evaluator
   * <p/>
   * Allocates one if its not already allocated
   *
   * @param evaluatorId
   * @return
   */
  public NumberedSplit<V> getInputSplit(final String hostName, final String evaluatorId) {
    synchronized (evaluatorToSplits) {
      if (evaluatorToSplits.containsKey(evaluatorId)) {
        LOG.log(Level.FINE, "Found an already allocated partition");
        LOG.log(Level.FINE, evaluatorToSplits.toString());
        return evaluatorToSplits.get(evaluatorId);
      }
    }
    LOG.log(Level.FINE, "allocated partition not found");
    if (locationToSplits.containsKey(hostName)) {
      LOG.log(Level.FINE, "Found partitions possibly hosted for " + evaluatorId + " at " + hostName);
      final NumberedSplit<V> split = allocateSplit(evaluatorId, locationToSplits.get(hostName));
      LOG.log(Level.FINE, evaluatorToSplits.toString());
      if (split != null) {
        return split;
      }
    }
    //pick random
    LOG.log(
        Level.FINE,
        hostName
            + " does not host any partitions or someone else took partitions hosted here. Picking a random one");
    final NumberedSplit<V> split = allocateSplit(evaluatorId, unallocatedSplits);
    LOG.log(Level.FINE, evaluatorToSplits.toString());
    if (split != null) {
      return split;
    }
    throw new RuntimeException("Unable to find an input partition to evaluator " + evaluatorId);
  }

  private NumberedSplit<V> allocateSplit(final String evaluatorId,
                                         final BlockingQueue<NumberedSplit<V>> value) {
    if (value == null) {
      LOG.log(Level.FINE, "Queue of splits can't be empty. Returning null");
      return null;
    }
    while (true) {
      final NumberedSplit<V> split = value.poll();
      if (split == null)
        return null;
      if (value == unallocatedSplits || unallocatedSplits.remove(split)) {
        LOG.log(Level.FINE, "Found split-" + split.getIndex() + " in the queue");
        final NumberedSplit<V> old = evaluatorToSplits.putIfAbsent(evaluatorId, split);
        if (old != null) {
          final String msg = "Trying to assign different partitions to the same evaluator " +
              "is not supported";
          LOG.severe(msg);
          throw new RuntimeException(msg);
        } else {
          LOG.log(Level.FINE, "Returning " + split.getIndex());
          return split;
        }
      }
    }
  }
}
