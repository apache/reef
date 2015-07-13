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
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

/**
 * Has multiple {@link LocationAwareJobConf}. Allows to iterate over them via an
 * Iterable interface.
 */
public final class LocationAwareJobConfs implements
    Iterable<LocationAwareJobConf> {

  private final List<LocationAwareJobConf> locationAwareJobConfs = new ArrayList<>();

  @Inject
  public LocationAwareJobConfs(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(DataPartitions.class) final Set<String> serializedDataPartitions) {
    Validate.notEmpty(inputFormatClassName);
    Validate.notEmpty(serializedDataPartitions);
    for (final String serializedDataPartition : serializedDataPartitions) {
      final DataPartition dp = DataPartitionSerializer.deserialize(serializedDataPartition);
      final ExternalConstructor<JobConf> jobConf = new JobConfExternalConstructor(inputFormatClassName,
            dp.getPath());
      locationAwareJobConfs.add(new LocationAwareJobConf(jobConf.newInstance(), dp));
    }
  }

  int size() {
    return locationAwareJobConfs.size();
  }

  @Override
  public Iterator<LocationAwareJobConf> iterator() {
    return new LocationAwareJobConfsIterator(locationAwareJobConfs);
  }

  static final class LocationAwareJobConfsIterator implements
      Iterator<LocationAwareJobConf> {

    private final List<LocationAwareJobConf> locationAwareJobConfs;
    private int position;

    public LocationAwareJobConfsIterator(
        final List<LocationAwareJobConf> locationAwareJobConfs) {
      this.locationAwareJobConfs = new LinkedList<LocationAwareJobConf>(
          locationAwareJobConfs);
      position = 0;
    }

    @Override
    public boolean hasNext() {
      return position < locationAwareJobConfs.size();
    }

    @Override
    public LocationAwareJobConf next() {
      final LocationAwareJobConf locationAwareJobConf = locationAwareJobConfs
          .get(position);
      position++;
      return locationAwareJobConf;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Remove method has not been implemented in this iterator");
    }
  }

  @NamedParameter
  public static final class DataPartitions implements Name<Set<String>> {
  }

}
