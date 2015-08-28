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
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.runtime.common.utils.Constants;
import org.apache.reef.tang.annotations.Parameter;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * This is an online version which satisfies requests based on the locations the
 * users ask the data to be loaded, for multiple data center network topologies.
 *
 */
@DriverSide
@Unstable
public final class MultiDataCenterEvaluatorToPartitionStrategy extends AbstractEvaluatorToPartitionStrategy {
  private static final Logger LOG = Logger.getLogger(MultiDataCenterEvaluatorToPartitionStrategy.class.getName());

  /**
   * Sorted set in reverse order, to keep track of the locations from most to
   * least specific. For example: [/dc1/room1, /dc1].
   */
  private Set<String> normalizedLocations;
  /**
   * Partial locations where we want to allocate, in case exact match does not work.
   */
  private ConcurrentMap<String, BlockingQueue<NumberedSplit<InputSplit>>> partialLocationsToSplits;


  @Inject
  MultiDataCenterEvaluatorToPartitionStrategy(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(DistributedDataSetPartitionSerializer.DistributedDataSetPartitions.class)
      final Set<String> serializedDataPartitions) {
    super(inputFormatClassName, serializedDataPartitions);
  }

  /**
   * Creates the objects to be used in updateLocations and tryAllocate methods.
   */
  @Override
  protected void setUp() {
    normalizedLocations = new TreeSet<>(Collections.reverseOrder());
    partialLocationsToSplits = new ConcurrentHashMap<>();
  }

  /**
   * {@inheritDoc}.
   * Saves locationToSplits and partialLocations as well.
   */
  @Override
  protected void updateLocations(final NumberedSplit<InputSplit> numberedSplit) {
    final String location = numberedSplit.getLocation();
    addLocationMapping(locationToSplits, numberedSplit, location);
    final String normalizedLocation = normalize(location);
    addLocationMapping(partialLocationsToSplits, numberedSplit, normalizedLocation);
    normalizedLocations.add(normalizedLocation);
  }

  /**
   * {@inheritDoc}. Tries to allocate on exact rack match, if it cannot, then it
   * tries to get a partial match using the partialLocations map.
   */
  @Override
  protected NumberedSplit<InputSplit> tryAllocate(final NodeDescriptor nodeDescriptor, final String evaluatorId) {
    final String rackName = nodeDescriptor.getRackDescriptor().getName();
    LOG.log(Level.FINE, "Trying an exact match on rack name {0}", rackName);
    if (locationToSplits.containsKey(rackName)) {
      LOG.log(Level.FINE, "Found splits possibly hosted for {0} at {1}", new Object[] {evaluatorId, rackName});
      final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId, locationToSplits.get(rackName));
      if (split != null) {
        return split;
      }
    }
    LOG.fine("No success, trying based on a partial match on locations");
    final Iterator<String> it = normalizedLocations.iterator();
    while (it.hasNext()) {
      final String possibleLocation = it.next();
      LOG.log(Level.FINE, "Trying on possible location {0}", possibleLocation);
      if (rackName.startsWith(possibleLocation)) {
        LOG.log(Level.FINE, "Found splits possibly hosted for {0} at {1} for rack {2}", new Object[] {evaluatorId,
            possibleLocation, rackName});
        final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId,
            partialLocationsToSplits.get(possibleLocation));
        if (split != null) {
          return split;
        }
      }
    }
    LOG.fine("Nothing found");
    return null;
  }

  private void addLocationMapping(final ConcurrentMap<String,
      BlockingQueue<NumberedSplit<InputSplit>>> concurrentMap,
      final NumberedSplit<InputSplit> numberedSplit, final String location) {
    if (!concurrentMap.containsKey(location)) {
      final BlockingQueue<NumberedSplit<InputSplit>> newSplitQueue = new LinkedBlockingQueue<>();
      concurrentMap.put(location, newSplitQueue);
    }
    concurrentMap.get(location).add(numberedSplit);
  }

  private String normalize(final String location) {
    String loc = location;
    // should start with a separator
    if (!loc.startsWith(Constants.RACK_PATH_SEPARATOR)) {
      loc = Constants.RACK_PATH_SEPARATOR + loc;
    }
    // if it is just /*, return /
    if (loc.equals(Constants.RACK_PATH_SEPARATOR + Constants.ANY_RACK)) {
      return Constants.RACK_PATH_SEPARATOR;
    }
    // remove the ending ANY or path separator
    while (loc.endsWith(Constants.ANY_RACK) || loc.endsWith(Constants.RACK_PATH_SEPARATOR)) {
      loc = loc.substring(0, loc.length() - 1);
    }
    return loc;
  }
}
