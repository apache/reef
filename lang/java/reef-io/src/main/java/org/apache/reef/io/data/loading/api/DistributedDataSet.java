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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Represents a distributed data set that is split across data centers.
 * It contains a set of distributed data set partitions {@link DistributedDataSetPartition}
 * this data to be loaded into.
 *
 */
@Unstable
public final class DistributedDataSet implements Iterable<DistributedDataSetPartition> {


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
  @SuppressWarnings("checkstyle:hiddenfield")
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

  @Override
  public Iterator<DistributedDataSetPartition> iterator() {
    return new DistributedDataSetIterator(partitions);
  }

  static final class DistributedDataSetIterator implements Iterator<DistributedDataSetPartition> {

    private final List<DistributedDataSetPartition> partitions;
    private int position;

    DistributedDataSetIterator(
        final Collection<DistributedDataSetPartition> partitions) {
      this.partitions = new LinkedList<>(partitions);
      position = 0;
    }

    @Override
    public boolean hasNext() {
      return position < partitions.size();
    }

    @Override
    public DistributedDataSetPartition next() {
      final DistributedDataSetPartition partition = partitions
          .get(position);
      position++;
      return partition;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Remove method has not been implemented in this iterator");
    }
  }
}
