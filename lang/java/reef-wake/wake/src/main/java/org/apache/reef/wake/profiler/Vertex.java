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
package org.apache.reef.wake.profiler;

import org.apache.reef.tang.types.ConstructorDef;

import java.util.Arrays;

/**
 * A vertex in the object graph.  There is no edge type, since that would be redundant.
 */
public class Vertex<T> {
  private final Object object;
  private final String name;
  private final ConstructorDef<T> constructorDef;
  private final Vertex<?>[] constructorArguments;

  public Vertex(final T object, final String name, final ConstructorDef<T> constructorDef,
                final Vertex<?>[] constructorArguments) {
    this.object = object;
    if (object == null) {
      throw new NullPointerException("The first argument of the Vertex constructor is null.");
    }
    this.name = name;
    this.constructorDef = constructorDef;
    this.constructorArguments = constructorArguments;
    for (final Vertex<?> v : constructorArguments) {
      if (v == null) {
        throw new NullPointerException("One of the vertices in the Vertex constructorArguments is null.");
      }
    }
  }

  public Vertex(final T object, final ConstructorDef<T> constructorDef, final Vertex<?>[] constructorArguments) {
    this.object = object;
    if (object == null) {
      throw new NullPointerException("The first argument of the Vertex constructor is null.");
    }
    this.name = null;
    this.constructorDef = constructorDef;
    this.constructorArguments = constructorArguments;
    for (final Vertex<?> v : constructorArguments) {
      if (v == null) {
        throw new NullPointerException("One of the vertices in the Vertex constructorArguments is null.");
      }
    }
  }

  public Vertex(final Object object) {
    this.object = object;
    if (object == null) {
      throw new NullPointerException("The argument of the Vertex constructor is null.");
    }
    this.name = null;
    this.constructorDef = null;
    this.constructorArguments = null;
  }

  public ConstructorDef<T> getConstructorDef() {
    return this.constructorDef;
  }

  public Vertex<?>[] getOutEdges() {
    if (constructorArguments == null) {
      return new Vertex[0];
    } else {
      return Arrays.copyOf(constructorArguments, constructorArguments.length);
    }
  }

  public Object getObject() {
    return object;
  }

  public String getName() {
    return name;
  }
}
