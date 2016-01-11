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

/**
 * A tuple of an object of type E and an integer index.
 * Used inside {@link org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy} implementations to
 * mark the partitions associated with each {@link org.apache.hadoop.mapred.InputSplit}
 *
 * @param <E>
 */
public final class NumberedSplit<E> implements Comparable<NumberedSplit<E>> {
  private final E entry;
  private final int index;
  private final DistributedDataSetPartition partition;

  public NumberedSplit(final E entry, final int index, final DistributedDataSetPartition partition) {
    Validate.notNull(entry, "Entry cannot be null");
    Validate.notNull(partition, "Partition cannot be null");
    this.entry = entry;
    this.index = index;
    this.partition = partition;
  }

  public String getPath() {
    return partition.getPath();
  }

  public String getLocation() {
    return partition.getLocation();
  }

  public E getEntry() {
    return entry;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "InputSplit-" + partition + "-" + index;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NumberedSplit<?> that = (NumberedSplit<?>) o;
    return index == that.index;
  }

  @Override
  public int hashCode() {
    return index;
  }

  @Override
  public int compareTo(final NumberedSplit<E> o) {
    if (this.index == o.index) {
      return 0;
    }
    if (this.index < o.index) {
      return -1;
    }
    return 1;
  }
}
