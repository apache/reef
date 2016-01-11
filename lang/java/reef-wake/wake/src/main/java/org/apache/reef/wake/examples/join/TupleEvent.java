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
package org.apache.reef.wake.examples.join;

/**
 * A tuple event consisting key and value pair.
 */
public class TupleEvent implements Comparable<TupleEvent> {
  private final int key;
  private final String val;

  public TupleEvent(final int key, final String val) {
    this.key = key;
    this.val = val;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TupleEvent that = (TupleEvent) o;

    if (key != that.key) {
      return false;
    }
    return val != null ? val.equals(that.val) : that.val == null;

  }

  @Override
  public int hashCode() {
    int result = key;
    result = 31 * result + (val != null ? val.hashCode() : 0);
    return result;
  }

  @Override
  public int compareTo(final TupleEvent o) {
    final int keycmp = Integer.compare(key, o.key);
    if (keycmp != 0) {
      return keycmp;
    }
    return val.compareTo(o.val);
  }

  @Override
  public String toString() {
    return "(" + key + ", " + val + ")";
  }

}
