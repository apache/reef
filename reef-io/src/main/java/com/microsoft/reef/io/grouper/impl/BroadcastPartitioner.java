package com.microsoft.reef.io.grouper.impl;
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
 * Basic partitioner that puts each key in all partitions
 */
public class BroadcastPartitioner<K> implements Partitioner<K> {
  //private final int num;

  public BroadcastPartitioner(int numPartitions) {
    //num = numPartitions;
  }

  @Override
  public int partition(K k) {
    return Partitioner.ALL;
  }
}
