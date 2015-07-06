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
package org.apache.reef.io.network.shuffle.topology;

import org.apache.reef.io.network.shuffle.grouping.Grouping;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 *
 */
public final class ImmutableGroupingDescription<K, V> implements GroupingDescription<K, V> {

  private final String groupingName;
  private final Class<? extends Grouping> groupingClass;
  private final String senderPoolId;
  private final String receiverPoolId;
  private final Class<? extends Codec<K>> keyCodecClass;
  private final Class<? extends Codec<V>> valueCodecClass;

  @Inject
  private ImmutableGroupingDescription(
      final @Parameter(GroupingName.class) String groupingName,
      final @Parameter(GroupingClassName.class) String groupingClassName,
      final @Parameter(SenderPoolId.class) String senderPoolId,
      final @Parameter(ReceiverPoolId.class) String receiverPoolId,
      final @Parameter(KeyCodecClassName.class) String keyCodecClassName,
      final @Parameter(ValueCodecClassName.class) String valueCodecClassName) {
    this.groupingName = groupingName;
    this.senderPoolId = senderPoolId;
    this.receiverPoolId = receiverPoolId;
    try {
      groupingClass = (Class<? extends Grouping>) Class.forName(groupingClassName);
      keyCodecClass = (Class<? extends Codec<K>>) Class.forName(keyCodecClassName);
      valueCodecClass = (Class<? extends Codec<V>>) Class.forName(valueCodecClassName);
    } catch (final ClassNotFoundException exception) {
      throw new RuntimeException("ClassNotFoundException in constructor of GroupingDescriptionImpl", exception);
    }
  }

  private ImmutableGroupingDescription(
      final String groupingName,
      final Class<? extends Grouping> groupingClass,
      final String senderPoolId,
      final String receiverPoolId,
      final Class<? extends Codec<K>> keyCodecClass,
      final Class<? extends Codec<V>> valueCodecClass) {

    this.groupingName = groupingName;
    this.groupingClass = groupingClass;
    this.senderPoolId = senderPoolId;
    this.receiverPoolId = receiverPoolId;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
  }

  @Override
  public String getGroupingName() {
    return groupingName;
  }

  @Override
  public Class<? extends Grouping> getGroupingClass() {
    return groupingClass;
  }

  @Override
  public String getSenderPoolId() {
    return senderPoolId;
  }

  @Override
  public String getReceiverPoolId() {
    return receiverPoolId;
  }

  @Override
  public Class<? extends Codec<K>> getKeyCodec() {
    return keyCodecClass;
  }

  @Override
  public Class<? extends Codec<V>> getValueCodec() {
    return valueCodecClass;
  }

  @Override
  public Configuration convertToConfiguration() {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindImplementation(GroupingDescription.class, ImmutableGroupingDescription.class);
    confBuilder.bindNamedParameter(GroupingName.class, groupingName);
    confBuilder.bindNamedParameter(GroupingClassName.class, groupingClass.getName());
    confBuilder.bindNamedParameter(SenderPoolId.class, senderPoolId);
    confBuilder.bindNamedParameter(ReceiverPoolId.class, receiverPoolId);
    confBuilder.bindNamedParameter(KeyCodecClassName.class, keyCodecClass.getName());
    confBuilder.bindNamedParameter(ValueCodecClassName.class, valueCodecClass.getName());
    return confBuilder.build();
  }

  public static Builder newBuilder(final String groupingName) {
    return new Builder(groupingName);
  }

  public static class Builder {
    private final String groupingName;
    private Class<? extends Grouping> groupingClass;
    private String senderPoolId;
    private String receiverPoolId;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;

    private Builder(final String groupingName) {
      this.groupingName = groupingName;
    }

    public Builder setGroupingClass(final Class<? extends Grouping> groupingClass) {
      this.groupingClass = groupingClass;
      return this;
    }

    public Builder setSenderPoolId(final String senderPoolId) {
      this.senderPoolId = senderPoolId;
      return this;
    }

    public Builder setReceiverPoolId(final String receiverPoolId) {
      this.receiverPoolId = receiverPoolId;
      return this;
    }

    public Builder setKeyCodecClass(final Class<? extends Codec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    public Builder setValueCodecClass(final Class<? extends Codec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    public GroupingDescription build() {
      if (senderPoolId == null) {
        throw new RuntimeException("You should set sender pool id");
      }

      if (receiverPoolId == null) {
        throw new RuntimeException("You should set receiver pool id");
      }

      if (groupingClass == null) {
        throw new RuntimeException("You should set grouping class");
      }

      if (keyCodecClass == null || valueCodecClass == null) {
        throw new RuntimeException("You should set codec for both key and value type");
      }

      return new ImmutableGroupingDescription(groupingName, groupingClass, senderPoolId,
          receiverPoolId, keyCodecClass, valueCodecClass);
    }
  }

  @NamedParameter
  public static class GroupingName implements Name<String> {
  }

  @NamedParameter
  public static class GroupingClassName implements Name<String> {
  }

  @NamedParameter
  public static class SenderPoolId implements Name<String> {
  }

  @NamedParameter
  public static class ReceiverPoolId implements Name<String> {
  }

  @NamedParameter
  public static class KeyCodecClassName implements Name<String> {
  }

  @NamedParameter
  public static class ValueCodecClassName implements Name<String> {
  }
}
