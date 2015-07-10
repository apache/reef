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

import org.apache.commons.lang.Validate;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.io.data.loading.api.EvaluatorToSplitStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * This is an abstract class useful for {@link EvaluatorToSplitStrategy}
 * implementations. Contains template methods that call abstract ones, that
 * should be implemented by subclasses. If your implementation does not need this
 * logic, you should just implement the {@link EvaluatorToSplitStrategy}
 * interface and do not extend this class.
 */
@DriverSide
public abstract class AbstractEvaluatorToSplitStrategy implements EvaluatorToSplitStrategy<InputSplit> {
  private static final Logger LOG = Logger
      .getLogger(AbstractEvaluatorToSplitStrategy.class.getName());

  protected final ConcurrentMap<String, BlockingQueue<NumberedSplit<InputSplit>>> locationToSplits;;
  protected final ConcurrentMap<String, NumberedSplit<InputSplit>> evaluatorToSplits;
  protected final BlockingQueue<NumberedSplit<InputSplit>> unallocatedSplits;


  @Inject
  AbstractEvaluatorToSplitStrategy() {
    LOG.fine("AbstractEvaluatorToSplitStrategy injected");
    locationToSplits = new ConcurrentHashMap<>();
    evaluatorToSplits =  new ConcurrentHashMap<>();
    unallocatedSplits = new LinkedBlockingQueue<>();
  }

  /**
   * Initializes the locations of the splits where we'd like to be loaded into.
   * Sets all the splits to unallocated
   *
   * @param splitsPerPartition
   *          a map containing the input splits per data partition
   */
  @Override
  public void init(final Map<DataPartition, InputSplit[]> splitsPerPartition) {
    final Pair<InputSplit[], DataPartition[]> splitsAndPartitions = getSplitsAndPartitions(splitsPerPartition);
    final InputSplit[] splits = splitsAndPartitions.getFirst();
    final DataPartition[] partitions = splitsAndPartitions.getSecond();
    Validate.isTrue(splits.length == partitions.length);
    for (int splitNum = 0; splitNum < splits.length; splitNum++) {
      LOG.log(Level.FINE, "Processing split: " + splitNum);
      final InputSplit split = splits[splitNum];
      final NumberedSplit<InputSplit> numberedSplit = new NumberedSplit<InputSplit>(split, splitNum,
          partitions[splitNum]);
      unallocatedSplits.add(numberedSplit);
      updateLocations(split, numberedSplit);
    }
    for (final Map.Entry<String, BlockingQueue<NumberedSplit<InputSplit>>> locSplit : locationToSplits.entrySet()) {
      LOG.log(Level.FINE, locSplit.getKey() + ": " + locSplit.getValue().toString());
    }
  }

  /**
   * Each strategy should update the locations where they want the split to be
   * loaded into. For example, the split physical location, certain node,
   * certain rack
   *
   * @param split
   *          the inputSplit
   * @param numberedSplit
   *          the numberedSplit
   */
  protected abstract void updateLocations(final InputSplit split, final NumberedSplit<InputSplit> numberedSplit);

  /**
   * Tries to allocate a split in an evaluator based on some particular rule.
   * For example, based on the rack name, randomly, etc.
   *
   * @param nodeDescriptor
   *          the node descriptor to extract information from
   * @param evaluatorId
   *          the evaluator id where we want to allocate the numberedSplit
   * @return a numberedSplit or null if couldn't allocate one
   */
  protected abstract NumberedSplit<InputSplit> tryAllocate(NodeDescriptor nodeDescriptor, String evaluatorId);

  /**
   * Get an input split to be assigned to this.
   * evaluator
   * <p/>
   * Allocates one if its not already allocated
   *
   * @param evaluatorId
   * @return a numberedSplit
   * @throws RuntimeException if couldn't find any split
   */
  @Override
  public NumberedSplit<InputSplit> getInputSplit(final NodeDescriptor nodeDescriptor, final String evaluatorId) {
    synchronized (evaluatorToSplits) {
      if (evaluatorToSplits.containsKey(evaluatorId)) {
        LOG.log(Level.FINE, "Found an already allocated split");
        LOG.log(Level.FINE, evaluatorToSplits.toString());
        return evaluatorToSplits.get(evaluatorId);
      }
    }
    // always first try to allocate based on the hostName
    final String hostName = nodeDescriptor.getName();
    LOG.log(Level.FINE, "Allocated split not found, trying on {0}", hostName);
    if (locationToSplits.containsKey(hostName)) {
      LOG.log(Level.FINE, "Found splits possibly hosted for {0} at {1}", new Object[] {evaluatorId, hostName});
      final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId, locationToSplits.get(hostName));
      if (split != null) {
        return split;
      }
    }
    LOG.log(Level.FINE,
        "{0} does not host any splits or someone else took splits hosted here. Picking other ones", hostName);
    final NumberedSplit<InputSplit> split = tryAllocate(nodeDescriptor, evaluatorId);
    if (split == null) {
      throw new RuntimeException("Unable to find an input split to evaluator " + evaluatorId);
    } else {
      LOG.log(Level.FINE, evaluatorToSplits.toString());
    }
    return split;
  }

  private Pair<InputSplit[], DataPartition[]> getSplitsAndPartitions(
      final Map<DataPartition, InputSplit[]> splitsPerPartition) {
    final List<InputSplit> inputSplits = new ArrayList<>();
    final List<DataPartition> partitions = new ArrayList<>();
    for (final Entry<DataPartition, InputSplit[]> entry : splitsPerPartition
        .entrySet()) {
      final DataPartition partition = entry.getKey();
      final InputSplit[] splits = entry.getValue();
      for (final InputSplit split : splits) {
        inputSplits.add(split);
        partitions.add(partition);
      }
    }
    return new Pair<>(inputSplits.toArray(new InputSplit[inputSplits.size()]),
        partitions.toArray(new DataPartition[partitions.size()]));
  }

  /**
   * Allocates the first available split into the evaluator.
   * @param evaluatorId
   *    the evaluator id
   * @param value
   *    the queue of splits
   * @return
   *    a numberedSplit or null if it cannot find one
   */
  protected NumberedSplit<InputSplit> allocateSplit(final String evaluatorId,
                                         final BlockingQueue<NumberedSplit<InputSplit>> value) {
    if (value == null) {
      LOG.log(Level.FINE, "Queue of splits can't be empty. Returning null");
      return null;
    }
    while (true) {
      final NumberedSplit<InputSplit> split = value.poll();
      if (split == null) {
        return null;
      }
      if (value == unallocatedSplits || unallocatedSplits.remove(split)) {
        LOG.log(Level.FINE, "Found split-" + split.getIndex() + " in the queue");
        final NumberedSplit<InputSplit> old = evaluatorToSplits.putIfAbsent(evaluatorId, split);
        if (old != null) {
          throw new RuntimeException("Trying to assign different splits to the same evaluator is not supported");
        } else {
          LOG.log(Level.FINE, "Returning " + split.getIndex());
          return split;
        }
      }
    }
  }
}
