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

/**
 * POJO that represents a data partition. Basically, it contains the path where
 * the data files are located for this partition, and the location where we want
 * this data to be loaded into.
 *
 */
public final class DataPartition {

  public static final String ANY = "/*";

  /**
   * The path of the folder.
   */
  private final String path;

  /**
   * The location (either a rackName or a nodeName) where we want the data
   * in this partition to be loaded into.
   */
  private final String location;

  public DataPartition(final String path, final String location) {
    this.path = path;
    this.location = location;
  }

  public String getPath() {
    return path;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DataPartition)) {
      return false;
    }
    final DataPartition that = (DataPartition) obj;
    return new EqualsBuilder().append(this.path, that.path)
        .append(this.location, that.location).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(this.path).append(this.location)
        .toHashCode();
  }

  public String toString() {
    return "{" + this.path + "," + this.location + "}";
  }
}
