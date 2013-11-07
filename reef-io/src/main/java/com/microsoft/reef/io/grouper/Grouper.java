package com.microsoft.reef.io.grouper;
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

import com.microsoft.wake.rx.Observable;
import com.microsoft.wake.rx.RxStage;


public interface Grouper<I> extends RxStage<I>, Observable {

  /**
   * Classes that implement this interface know how to map a key to destinations
   * 
   * Like hadoop.mapreduce.Partitioner, the partition is abstractly denoted as
   * an integer, which may be used in another structure to lookup a destination
   * Identifier.
   */
  public interface Partitioner<K> {
    public static final int ALL = -1;
    public static final int NONE = -2;

    /**
     * Get the partitions where a given key should be sent. Partitions are
     * abstractly represented as contiguous integers.
     */
    public int partition(K k);
  }

  public interface Combiner<OutType, K, V> {
    /**
     * Combine two values. For general Grouper implementations, this operation
     * should be commutative and associative to guarantee deterministic results.
     * Take a key, the associated current value for the key, and a new value to
     * accumulate. Returns the new value for the key.
     * 
     * @param key
     *          the key associated with the combined values
     * @param sofar
     *          must not be {@code null} unless this has semantic meaning for
     *          type V
     * @param val
     *          new value to combine
     * 
     * @return result of the combining {@code sofar} and {@code val}
     */
    V combine(K key, V sofar, V val);

    /**
     * Get the final value of the Combiner. The OutType is often
     * just a pair of key, val, or the same as InType
     */
    OutType generate(K key, V val);
   
    /**
     * Returns true if this value is fully baked.
     * 
     * The Grouper may then, e.g., decide to remove and output the key
     */
    boolean keyFinished(K key, V val);
  }

  /**
   * Extracts key and value fields from IT
   */
  public interface Extractor<InType, K, V> {
    /**
     * Extract the key on which to group records by in the Grouper
     */
    public K key(InType t);

    /**
     * Extract the value which will be combined.
     */
    public V value(InType t);
  }

  public enum SynchronizationPolicy {
    /**
     * The grouper may send each key many times (e.g. snowshovel)
     */
    ASYNC,

    /**
     * The grouper may send a given key when all instances of it have been seen.
     * (e.g. useful when input is sorted)
     */
    SINGLE_KEY,

    /**
     * The grouper may send keys once all instances of the keys for a given
     * partition have been seen.
     */
    ALL_KEYS_PER_PARTITION,

    /**
     * The grouper sends only when all inputs have been seen. (e.g. MapReduce
     * shuffle)
     */
    ALL_KEYS
  }

}
