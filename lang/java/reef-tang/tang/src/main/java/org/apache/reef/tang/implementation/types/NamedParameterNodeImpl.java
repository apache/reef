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
package org.apache.reef.tang.implementation.types;

import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;

public class NamedParameterNodeImpl<T> extends AbstractNode implements
    NamedParameterNode<T> {
  private final String fullArgName;
  private final String simpleArgName;
  private final String documentation;
  private final String shortName;
  private final String[] defaultInstanceAsStrings;
  private final boolean isSet;
  private final boolean isList;

  public NamedParameterNodeImpl(Node parent, String simpleName,
                                String fullName, String fullArgName, String simpleArgName, boolean isSet, boolean isList,
                                String documentation, String shortName, String[] defaultInstanceAsStrings) {
    super(parent, simpleName, fullName);
    this.fullArgName = fullArgName;
    this.simpleArgName = simpleArgName;
    this.isSet = isSet;
    this.isList = isList;
    this.documentation = documentation;
    this.shortName = shortName;
    this.defaultInstanceAsStrings = defaultInstanceAsStrings;
  }

  @Override
  public String toString() {
    return getSimpleArgName() + " " + getName();
  }

  @Override
  public String getSimpleArgName() {
    return simpleArgName;
  }

  @Override
  public String getFullArgName() {
    return fullArgName;
  }

  @Override
  public String getDocumentation() {
    return documentation;
  }

  @Override
  public String getShortName() {
    return shortName;
  }

  @Override
  public String[] getDefaultInstanceAsStrings() {
    return defaultInstanceAsStrings;
  }

  @Override
  public boolean isSet() {
    return isSet;
  }

  @Override
  public boolean isList() {
    return isList;
  }
}