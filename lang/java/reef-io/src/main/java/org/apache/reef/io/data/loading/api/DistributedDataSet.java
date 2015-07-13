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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a distributed data set that is split across data centers.
 * It contains a set of distributed data set partitions {@link DistributedDataSetPartition}
 * this data to be loaded into.
 *
 */
@Unstable
public final class DistributedDataSet {


  /**
   * The set of distributed data set partitions.
   */
  private final Set<DistributedDataSetPartition> partitions = new HashSet<>();

  /**
   * Adds the given partition to the set.
   *
   * @param partition
   *          the partition to add
   */
  public void addPartition(final DistributedDataSetPartition partition) {
    this.partitions.add(partition);
  }

  /**
   * Adds the given partitions to the set.
   *
   * @param partitions
   *          the partitions to add
   */
  public void addPartitions(final Collection<DistributedDataSetPartition> partitions) {
    this.partitions.addAll(partitions);
  }

  /**
   * Returns true if it does not contain any partition.
   *
   * @return a boolean indicating whether it contains partitions or not
   */
  public boolean isEmpty() {
    return this.partitions.isEmpty();
  }

  /**
   * Returns the set of partitions this data set has.
   *
   * @return an unmodifiable set of partitions
   */
  public Set<DistributedDataSetPartition> getPartitions() {
    return Collections.unmodifiableSet(this.partitions);
  }
}
