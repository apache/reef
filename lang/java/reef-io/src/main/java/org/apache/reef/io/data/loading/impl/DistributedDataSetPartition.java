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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.reef.annotations.Unstable;

/**
 * POJO that represents a distributed data set partition. Basically, it contains the path where
 * the data files are located for this partition, and the location where we want
 * this data to be loaded into.
 *
 */
@Unstable
public final class DistributedDataSetPartition {

  /**
   * The path of the distributed data set partition. If we use HDFS, it will be the
   * hdfs path.
   */
  private final String path;

  /**
   * The location (either a rackName or a nodeName) where we want the data in
   * this distributed partition to be loaded into. It can contain a wildcard at
   * the end, for example /datacenter1/*.
   */
  private final String location;

  /**
   * Number of desired splits for this partition.
   */
  private final int desiredSplits;

  DistributedDataSetPartition(final String path, final String location, final int desiredSplits) {
    this.path = path;
    this.location = location;
    this.desiredSplits = desiredSplits;
  }

  /**
   * Returns the path to the distributed data partition.
   *
   * @return the path of the distributed data partition
   */
  String getPath() {
    return path;
  }

  /**
   * Returns the location where we want the data in this partition to be loaded
   * into.
   *
   * @return the location where to load this data into.
   */
  String getLocation() {
    return location;
  }

  /**
   * Returns the number of desired splits for this data partition.
   *
   * @return the number of desired splits
   */
  int getDesiredSplits() {
    return desiredSplits;
  }

  /**
   * @return a new DistributedDataSetPartition Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DistributedDataSetPartition)) {
      return false;
    }
    final DistributedDataSetPartition that = (DistributedDataSetPartition) obj;
    return new EqualsBuilder().append(this.path, that.path).append(this.location, that.location)
        .append(this.desiredSplits, that.desiredSplits).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(this.path).append(this.location).append(this.desiredSplits).toHashCode();
  }

  @Override
  public String toString() {
    return "{" + this.path + "," + this.location + "," + this.desiredSplits + "}";
  }

  /**
   * {@link DistributedDataSetPartition}s are build using this Builder.
   */
  public static final class Builder implements org.apache.reef.util.Builder<DistributedDataSetPartition> {

    private String path;
    private String location;
    private int desiredSplits;

    private Builder() {
    }

    /**
     * Sets the path of the distributed data set partition.
     *
     * @param path
     *          the path to set
     * @return this
     */
    public Builder setPath(final String path) {
      this.path = path;
      return this;
    }


    /**
     * Sets the location where we want the data in this partition to be loaded
     * into.
     *
     * @param location
     *          the location to set
     * @return this
     */
    public Builder setLocation(final String location) {
      this.location = location;
      return this;
    }

    /**
     * Sets the desired number of splits for this partition.
     * @param desiredSplits
     *          the number of desired splits
     * @return this
     */
    public Builder setDesiredSplits(final int desiredSplits) {
      this.desiredSplits = desiredSplits;
      return this;
    }

    /**
     * Builds the {@link DistributedDataSetPartition}.
     */
    @Override
    public DistributedDataSetPartition build() {
      return new DistributedDataSetPartition(this.path, this.location, this.desiredSplits);
    }
  }
}
