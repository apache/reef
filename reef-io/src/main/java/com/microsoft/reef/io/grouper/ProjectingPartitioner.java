package com.microsoft.reef.io.grouper;
/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.io.grouper.Grouper.Partitioner;

/**
 * Interface for partition functions that map keys to single partitions
 */
public abstract class ProjectingPartitioner<K> implements Partitioner<K> {

  @Override
  public final int partition(K k) {
    int p = projectingPartition(k);
    assert p != ALL && p != NONE;
    return p;
  }

  public abstract int projectingPartition(K k);
}
