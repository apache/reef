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
package org.apache.reef.examples.group.utils.math;


/**
 * A sparse vector represented by an index and value array.
 */
public final class SparseVector extends AbstractImmutableVector {

  private final double[] values;
  private final int[] indices;
  private final int size;


  public SparseVector(final double[] values, final int[] indices, final int size) {
    this.values = values;
    this.indices = indices;
    this.size = size;
  }

  public SparseVector(final double[] values, final int[] indices) {
    this(values, indices, -1);
  }


  @Override
  public double get(final int index) {
    for (int i = 0; i < indices.length; ++i) {
      if (indices[i] == index) {
        return values[i];
      }
    }
    return 0;
  }

  @Override
  public int size() {
    return this.size;
  }
}
