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

import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 *
 */
public final class ShuffleDescriptionImpl<K, V> implements ShuffleDescription<K, V> {

  private final String shuffleName;
  private final Class<? extends ShuffleStrategy> shuffleStrategyClass;
  private final Class<? extends Codec<K>> keyCodecClass;
  private final Class<? extends Codec<V>> valueCodecClass;

  @Inject
  private ShuffleDescriptionImpl(
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.ShuffleStrategyClassName.class) final String shuffleStrategyClassName,
      @Parameter(ShuffleParameters.ShuffleKeyCodecClassName.class) final String keyCodecClassName,
      @Parameter(ShuffleParameters.ShuffleValueCodecClassName.class) final String valueCodecClassName) {
    this.shuffleName = shuffleName;
    try {
      shuffleStrategyClass = (Class<? extends ShuffleStrategy>) Class.forName(shuffleStrategyClassName);
      keyCodecClass = (Class<? extends Codec<K>>) Class.forName(keyCodecClassName);
      valueCodecClass = (Class<? extends Codec<V>>) Class.forName(valueCodecClassName);
    } catch (final ClassNotFoundException exception) {
      throw new RuntimeException("ClassNotFoundException occurred in constructor of ShuffleDescriptionImpl", exception);
    }
  }

  private ShuffleDescriptionImpl(
      final String shuffleName,
      final Class<? extends ShuffleStrategy> shuffleStrategyClass,
      final Class<? extends Codec<K>> keyCodecClass,
      final Class<? extends Codec<V>> valueCodecClass) {
    this.shuffleName = shuffleName;
    this.shuffleStrategyClass = shuffleStrategyClass;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
  }

  @Override
  public String getShuffleName() {
    return shuffleName;
  }

  @Override
  public Class<? extends ShuffleStrategy> getShuffleStrategyClass() {
    return shuffleStrategyClass;
  }

  @Override
  public Class<? extends Codec<K>> getKeyCodecClass() {
    return keyCodecClass;
  }

  @Override
  public Class<? extends Codec<V>> getValueCodecClass() {
    return valueCodecClass;
  }

  public static Builder newBuilder(final String shuffleName) {
    return new Builder(shuffleName);
  }

  public static final class Builder {
    private final String shuffleName;
    private Class<? extends ShuffleStrategy> shuffleStrategyClass;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;

    private Builder(final String shuffleName) {
      this.shuffleName = shuffleName;
    }

    public Builder setShuffleStrategy(final Class<? extends ShuffleStrategy> shuffleStrategyClass) {
      this.shuffleStrategyClass = shuffleStrategyClass;
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

    public ShuffleDescription build() {
      if (shuffleStrategyClass == null) {
        throw new RuntimeException("You should set strategy class");
      }

      if (keyCodecClass == null || valueCodecClass == null) {
        throw new RuntimeException("You should set codec for both key and value type");
      }

      return new ShuffleDescriptionImpl(
          shuffleName, shuffleStrategyClass, keyCodecClass, valueCodecClass);
    }
  }
}
