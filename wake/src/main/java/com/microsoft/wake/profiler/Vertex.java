/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.wake.profiler;

import java.util.Arrays;

import com.microsoft.tang.types.ConstructorDef;

/**
 * A vertex in the object graph.  There is no edge type, since that would be redundant.
 */
public class Vertex<T> {
  private final Object object;
  private final String name;
  private final ConstructorDef<T> constructorDef;
  private final Vertex<?>[] constructorArguments;
//  private final Set<Object> referencesToThisObject = new MonotonicHashSet<>();
  
  public Vertex(T object, String name, ConstructorDef<T> constructorDef, Vertex<?>[] constructorArguments) {
    this.object = object;
    if(object == null) { throw new NullPointerException(); }
    this.name = name;
    this.constructorDef = constructorDef;
    this.constructorArguments = constructorArguments;
    for(Vertex<?> v : constructorArguments) {
      if(v == null) {
        throw new NullPointerException();
      }
    }
  }
  public Vertex(T object, ConstructorDef<T> constructorDef, Vertex<?>[] constructorArguments) {
    this.object = object;
    if(object == null) { throw new NullPointerException(); }
    this.name = null;
    this.constructorDef = constructorDef;
    this.constructorArguments = constructorArguments;
    for(Vertex<?> v : constructorArguments) {
      if(v == null) {
        throw new NullPointerException();
      }
    }
  }
  public Vertex(Object object) {
    this.object = object;
    if(object == null) { throw new NullPointerException(); }
    this.name = null;
    this.constructorDef = null;
    this.constructorArguments = null;
  }
//  public void addReference(Vertex<?> v) {
//    referencesToThisObject.add(v);
//  }
//  public Vertex<?>[] getInEdges() {
//    return referencesToThisObject.toArray(new Vertex[0]);
//  }
  public ConstructorDef<T> getConstructorDef() {
    return this.constructorDef;
  }
  public Vertex<?>[] getOutEdges() {
    if(constructorArguments == null) {
      return new Vertex[0];
    } else {
      return Arrays.copyOf(constructorArguments,  constructorArguments.length);
    }
  }
  public Object getObject() {
    return object;
  }
  public String getName() {
    return name;
  }
}
