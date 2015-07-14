/**
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
package org.apache.reef.io.network.shuffle.description;

import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.params.GroupingKeyCodecClassName;
import org.apache.reef.io.network.shuffle.params.GroupingName;
import org.apache.reef.io.network.shuffle.params.GroupingStrategyClassName;
import org.apache.reef.io.network.shuffle.params.GroupingValueCodecClassName;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 *
 */
public final class GroupingDescriptionImpl<K, V> implements GroupingDescription<K, V> {

  private final String groupingName;
  private final Class<? extends GroupingStrategy> groupingClass;
  private final Class<? extends Codec<K>> keyCodecClass;
  private final Class<? extends Codec<V>> valueCodecClass;

  @Inject
  private GroupingDescriptionImpl(
      final @Parameter(GroupingName.class) String groupingName,
      final @Parameter(GroupingStrategyClassName.class) String groupingClassName,
      final @Parameter(GroupingKeyCodecClassName.class) String keyCodecClassName,
      final @Parameter(GroupingValueCodecClassName.class) String valueCodecClassName) {
    this.groupingName = groupingName;
    try {
      groupingClass = (Class<? extends GroupingStrategy>) Class.forName(groupingClassName);
      keyCodecClass = (Class<? extends Codec<K>>) Class.forName(keyCodecClassName);
      valueCodecClass = (Class<? extends Codec<V>>) Class.forName(valueCodecClassName);
    } catch (final ClassNotFoundException exception) {
      throw new RuntimeException("ClassNotFoundException in constructor of GroupingDescriptionImpl", exception);
    }
  }

  private GroupingDescriptionImpl(
      final String groupingName,
      final Class<? extends GroupingStrategy> groupingClass,
      final Class<? extends Codec<K>> keyCodecClass,
      final Class<? extends Codec<V>> valueCodecClass) {

    this.groupingName = groupingName;
    this.groupingClass = groupingClass;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
  }

  @Override
  public String getGroupingName() {
    return groupingName;
  }

  @Override
  public Class<? extends GroupingStrategy> getGroupingStrategyClass() {
    return groupingClass;
  }

  @Override
  public Class<? extends Codec<K>> getKeyCodec() {
    return keyCodecClass;
  }

  @Override
  public Class<? extends Codec<V>> getValueCodec() {
    return valueCodecClass;
  }

  public static Builder newBuilder(final String groupingName) {
    return new Builder(groupingName);
  }

  public static class Builder {
    private final String groupingName;
    private Class<? extends GroupingStrategy> groupingClass;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;

    private Builder(final String groupingName) {
      this.groupingName = groupingName;
    }

    public Builder setGroupingStrategy(final Class<? extends GroupingStrategy> groupingClass) {
      this.groupingClass = groupingClass;
      return this;
    }

    public Builder setKeyCodec(final Class<? extends Codec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    public Builder setValueCodec(final Class<? extends Codec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    public GroupingDescription build() {
      if (groupingClass == null) {
        throw new RuntimeException("You should set grouping class");
      }

      if (keyCodecClass == null || valueCodecClass == null) {
        throw new RuntimeException("You should set codec for both key and value type");
      }

      return new GroupingDescriptionImpl(groupingName, groupingClass, keyCodecClass, valueCodecClass);
    }
  }
}
