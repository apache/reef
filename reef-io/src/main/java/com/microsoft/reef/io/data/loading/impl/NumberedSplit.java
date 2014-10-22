/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.data.loading.impl;

import org.apache.hadoop.mapred.InputSplit;

/**
 * A tuple of an object of type E and an integer index
 * Used inside {@link EvaluatorToPartitionMapper} to
 * mark the partitions associated with each {@link InputSplit}
 * 
 * @param <E>
 */
final class NumberedSplit<E> implements Comparable<NumberedSplit<E>> {
  private final E entry;
  private final int index;

  public NumberedSplit(final E entry, final int index) {
    super();
    if (entry == null) {
      throw new IllegalArgumentException("Entry cannot be null");
    }
    this.entry = entry;
    this.index = index;
  }

  public E getEntry() {
    return entry;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "InputSplit-" + index;
  }

  @Override
  public int compareTo(final NumberedSplit<E> o) {
    if (this.index == o.index)
      return 0;
    if (this.index < o.index)
      return -1;
    else
      return 1;
  }
}