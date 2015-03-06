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
 * Abstract base class for {@link Vector} implementations.
 * <p/>
 * The only methods to be implemented by subclasses are get, set and size.
 */
public abstract class AbstractVector extends AbstractImmutableVector implements Vector {

  @Override
  public abstract void set(int i, double v);


  @Override
  public void add(final Vector that) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) + that.get(index));
    }
  }

  @Override
  public void multAdd(final double factor, final ImmutableVector that) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) + factor * that.get(index));
    }
  }

  @Override
  public void scale(final double factor) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) * factor);
    }
  }


  @Override
  public void normalize() {
    final double factor = 1.0 / this.norm2();
    this.scale(factor);
  }


}
