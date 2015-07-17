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

import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.operator.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.operator.TupleSender;
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
  private final Class<? extends TupleSender<K, V>> tupleSenderClass;
  private final Class<? extends TupleReceiver<K, V>> tupleReceiverClass;
  private final Class<? extends GroupingController> groupingControllerClass;

  @Inject
  private GroupingDescriptionImpl(
      final @Parameter(GroupingName.class) String groupingName,
      final @Parameter(GroupingStrategyClassName.class) String groupingClassName,
      final @Parameter(GroupingKeyCodecClassName.class) String keyCodecClassName,
      final @Parameter(GroupingValueCodecClassName.class) String valueCodecClassName,
      final @Parameter(GroupingTupleSenderClassName.class) String tupleSenderClassName,
      final @Parameter(GroupingTupleReceiverClassName.class) String tupleReceiverClassName,
      final @Parameter(GroupingControllerClassName.class) String groupingControllerClassName) {
    this.groupingName = groupingName;
    try {
      groupingClass = (Class<? extends GroupingStrategy>) Class.forName(groupingClassName);
      keyCodecClass = (Class<? extends Codec<K>>) Class.forName(keyCodecClassName);
      valueCodecClass = (Class<? extends Codec<V>>) Class.forName(valueCodecClassName);
      if (tupleSenderClassName.equals("DEFAULT")) {
        this.tupleSenderClass = null;
      } else {
        this.tupleSenderClass = (Class<? extends TupleSender<K, V>>) Class.forName(tupleSenderClassName);
      }

      if (tupleReceiverClassName.equals("DEFAULT")) {
        this.tupleReceiverClass = null;
      } else {
        this.tupleReceiverClass = (Class<? extends TupleReceiver<K, V>>) Class.forName(tupleReceiverClassName);
      }

      if (groupingControllerClassName.equals("DEFAULT")) {
        this.groupingControllerClass = null;
      } else {
        this.groupingControllerClass = (Class<? extends GroupingController>) Class.forName(groupingControllerClassName);
      }
    } catch (final ClassNotFoundException exception) {
      throw new RuntimeException("ClassNotFoundException in constructor of GroupingDescriptionImpl", exception);
    }
  }

  private GroupingDescriptionImpl(
      final String groupingName,
      final Class<? extends GroupingStrategy> groupingClass,
      final Class<? extends Codec<K>> keyCodecClass,
      final Class<? extends Codec<V>> valueCodecClass,
      final Class<? extends TupleSender<K, V>> tupleSenderClass,
      final Class<? extends TupleReceiver<K, V>> tupleReceiverClass,
      final Class<? extends GroupingController> groupingControllerClass) {

    this.groupingName = groupingName;
    this.groupingClass = groupingClass;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
    this.tupleSenderClass = tupleSenderClass;
    this.tupleReceiverClass = tupleReceiverClass;
    this.groupingControllerClass = groupingControllerClass;
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
  public Class<? extends Codec<K>> getKeyCodecClass() {
    return keyCodecClass;
  }

  @Override
  public Class<? extends Codec<V>> getValueCodecClass() {
    return valueCodecClass;
  }

  @Override
  public Class<? extends TupleSender<K, V>> getTupleSenderClass() {
    return tupleSenderClass;
  }

  @Override
  public Class<? extends TupleReceiver<K, V>> getTupleReceiverClass() {
    return tupleReceiverClass;
  }

  @Override
  public Class<? extends GroupingController> getGroupingControllerClass() {
    return groupingControllerClass;
  }

  public static Builder newBuilder(final String groupingName) {
    return new Builder(groupingName);
  }

  public static class Builder {
    private final String groupingName;
    private Class<? extends GroupingStrategy> groupingClass;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;
    private Class<? extends TupleSender> tupleSenderClass;
    private Class<? extends TupleReceiver> tupleReceiverClass;
    private Class<? extends GroupingController> groupingControllerClass;

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

    public Builder setTupleSender(final Class<? extends TupleSender> tupleSenderClass) {
      this.tupleSenderClass = tupleSenderClass;
      return this;
    }

    public Builder setTupleReceiver(final Class<? extends TupleReceiver> tupleReceiverClass) {
      this.tupleReceiverClass = tupleReceiverClass;
      return this;
    }

    public Builder setGroupingController(final Class<? extends GroupingController> groupingControllerClass) {
      this.groupingControllerClass = groupingControllerClass;
      return this;
    }

    public GroupingDescription build() {
      if (groupingClass == null) {
        throw new RuntimeException("You should set grouping class");
      }

      if (keyCodecClass == null || valueCodecClass == null) {
        throw new RuntimeException("You should set codec for both key and value type");
      }

      return new GroupingDescriptionImpl(
          groupingName, groupingClass, keyCodecClass, valueCodecClass,
          tupleSenderClass, tupleReceiverClass, groupingControllerClass);
    }
  }
}
