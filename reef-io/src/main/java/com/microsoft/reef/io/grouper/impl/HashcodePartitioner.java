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

import com.microsoft.reef.io.grouper.ProjectingPartitioner;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Basic partitioner that uses a K.hashcode to pick partitions
 */
public class HashcodePartitioner<K> extends ProjectingPartitioner<K> {
  private final int num;

  @NamedParameter()
  public static class NumPartitions implements Name<Integer> {
  }

  @Inject
  public HashcodePartitioner(@Parameter(NumPartitions.class) int numPartitions) {
    num = numPartitions;
  }

  @Override
  public int projectingPartition(K k) {
    return k.hashCode() % num;
  }
}
