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
package com.microsoft.tang.implementation.types;

import java.util.Collection;
import java.util.Map;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicTreeMap;

public abstract class AbstractNode implements Node {
  @Override
  public Collection<Node> getChildren() {
    return children.values();
  }

  private final Node parent;
  private final String name;
  private final String fullName;
  protected final Map<String, Node> children = new MonotonicTreeMap<>();

  @Override
  public String getFullName() {
    return fullName;
  }

  @Override
  public boolean equals(Object o) {
    if(o == null) return false;
    if(o == this) return true;
    
    AbstractNode n = (AbstractNode) o;
    final boolean parentsEqual;
    if (n.parent == this.parent) {
      parentsEqual = true;
    } else if (n.parent == null) {
      parentsEqual = false;
    } else if (this.parent == null) {
      parentsEqual = false;
    } else {
      parentsEqual = n.parent.equals(this.parent);
    }
    if (!parentsEqual) {
      return false;
    }
    return this.name.equals(n.name);
  }

  public AbstractNode(Node parent, String name, String fullName) {
    this.parent = parent;
    this.name = name;
    this.fullName = fullName;
    if (parent != null) {
      if (name.length() == 0) {
        throw new IllegalArgumentException(
            "Zero length child name means bad news");
      }
      parent.put(this);
    }
  }

  @Override 
  public Node getParent() {
    return parent;
  }
  
  @Override
  public boolean contains(String key) {
    return children.containsKey(key);
  }

  @Override
  public Node get(String key) {
    return children.get(key);
  }

  @Override
  public void put(Node n) {
    children.put(n.getName(), n);
  }

  @SuppressWarnings("unused")
  private String toIndentedString(int level) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("\t");
    }
    sb.append(toString() + "\n");
    if (children != null) {
      for (Node n : children.values()) {
        sb.append(((AbstractNode)n).toIndentedString(level + 1));
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() + " '" + getFullName() + "']";
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int compareTo(Node n) {
    return getFullName().compareTo(n.getFullName());
  }
}