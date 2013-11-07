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
package com.microsoft.reef.io.grouper.impl;

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Countable;
import com.microsoft.reef.io.grouper.Grouper;
import com.microsoft.reef.io.grouper.Grouper.Combiner;
import com.microsoft.reef.io.grouper.Grouper.Extractor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

public class KeyCountGrouperUtils {

  /**
   * Combiner for CountedPairs when the running total is decremented
   * <p/>
   * The key is considered finished when count reaches 0.
   * <p/>
   * Note that because subtraction is not commutative, usually when this Combiner is used,
   * the first onNext(k') for key k' is special.
   */
  public static class DownCountedPairCombiner<K extends Comparable<K>, V> implements Combiner<CountedPair<K, V>, K, ValueAndCount<V>> {
    @NamedParameter(doc = "Combiner for the values")
    public static class ValueCombiner implements Name<Grouper.Combiner> {
    }

    private final Grouper.Combiner<Tuple<K, V>, K, V> valueCombiner;

    @Inject
    public DownCountedPairCombiner(@Parameter(ValueCombiner.class) Grouper.Combiner<Tuple<K, V>, K, V> valueCombiner) {
      this.valueCombiner = valueCombiner;
    }

    @Override
    public ValueAndCount<V> combine(K key, ValueAndCount<V> sofar, ValueAndCount<V> val) {
      return new ValueAndCount<>(valueCombiner.combine(key, sofar.value, val.value), sofar.count - val.count);
    }

    @Override
    public CountedPair<K, V> generate(K key, ValueAndCount<V> val) {
      Tuple<K, V> generatedTuple = valueCombiner.generate(key, val.value);
      return new CountedPair<>(generatedTuple.getKey(), generatedTuple.getValue(), val.count);
    }

    @Override
    public boolean keyFinished(K key, ValueAndCount<V> val) {
      return val.count == 0;
    }
  }

  /**
   * Combiner for CountedPairs that keeps a cumulative count
   */
  public static class UpCountedPairCombiner<K extends Comparable<K>, V> implements Combiner<CountedPair<K, V>, K, ValueAndCount<V>> {
    @NamedParameter(doc = "Combiner for the values")
    public static class ValueCombiner implements Name<Grouper.Combiner> {
    }

    private final Grouper.Combiner<Tuple<K, V>, K, V> valueCombiner;

    @Inject
    public UpCountedPairCombiner(@Parameter(ValueCombiner.class) Grouper.Combiner<Tuple<K, V>, K, V> valueCombiner) {
      this.valueCombiner = valueCombiner;
    }

    @Override
    public ValueAndCount<V> combine(K key, ValueAndCount<V> sofar, ValueAndCount<V> val) {
      return new ValueAndCount<>(valueCombiner.combine(key, sofar.value, val.value), sofar.count + val.count);
    }

    @Override
    public CountedPair<K, V> generate(K key, ValueAndCount<V> val) {
      Tuple<K, V> generatedTuple = valueCombiner.generate(key, val.value);
      return new CountedPair<>(generatedTuple.getKey(), generatedTuple.getValue(), val.count);
    }

    @Override
    public boolean keyFinished(K key, ValueAndCount<V> val) {
      return false;
    }
  }


  public static class CountedPairExtractor<K, V> implements Extractor<CountedPair<K, V>, K, ValueAndCount<V>> {

    @Inject
    public CountedPairExtractor() {
    }


    @Override
    public ValueAndCount<V> value(CountedPair<K, V> t) {
      return new ValueAndCount<>(t.value, t.count);
    }


    @Override
    public K key(CountedPair<K, V> t) {
      return t.key;
    }
  }


  public static class CountedPair<K, V> implements Countable {
    public final K key;
    public final V value;
    private final int count;

    public CountedPair(K key, V value, int count) {
      this.key = key;
      this.value = value;
      this.count = count;
    }

    @Override
    public int count() {
      return count;
    }

    @Override
    public String toString() {
      return "[key=" + key + ", value=" + value + ", count=" + count + "]";
    }
  }


  private static class ValueAndCount<V> {
    public final int count;
    public final V value;

    public ValueAndCount(V value, int count) {
      this.count = count;
      this.value = value;
    }

    @Override
    public String toString() {
      return "[value=" + value + ", count=" + count + "]";
    }
  }

}
