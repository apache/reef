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

import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * External constructor used by tang to create {@link LocationAwareJobConfs} objects.
 */
public class LocationAwareJobConfsExternalConstructor implements ExternalConstructor<LocationAwareJobConfs> {

  private final String inputFormatClassName;
  private final List<DataPartition> dataPartitions;

  @Inject
  public LocationAwareJobConfsExternalConstructor(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(DataPartitions.class) final Set<String> serializedDataPartitions) {
    this.inputFormatClassName = inputFormatClassName;
    this.dataPartitions = new ArrayList<>(serializedDataPartitions.size());
    for (final String serializedDataPartition : serializedDataPartitions) {
      this.dataPartitions.add(DataPartitionSerializer.deserialize(serializedDataPartition));
    }
  }

  @Override
  public LocationAwareJobConfs newInstance() {
    final List<LocationAwareJobConf> locationAwareJobConfs = new ArrayList<>(dataPartitions.size());
    final Iterator<DataPartition> it = dataPartitions.iterator();
    while (it.hasNext()) {
      final DataPartition dataPartition = it.next();
      final ExternalConstructor<JobConf> jobConf = new JobConfExternalConstructor(inputFormatClassName,
          dataPartition.getPath());
      locationAwareJobConfs.add(new LocationAwareJobConf(jobConf.newInstance(), dataPartition));
    }
    return new LocationAwareJobConfs(locationAwareJobConfs);
  }

  @NamedParameter
  public static final class DataPartitions implements Name<Set<String>> {
  }
}
