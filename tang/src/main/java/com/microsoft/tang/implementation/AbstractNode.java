package com.microsoft.tang.implementation;

import java.util.Collection;
import java.util.Map;

import com.microsoft.tang.Node;
import com.microsoft.tang.util.MonotonicMap;

public abstract class AbstractNode implements Node {
  @Override
  public Collection<Node> getChildren() {
    return children.values();
  }

  private final Node parent;
  private final String name;
  private final Map<String, Node> children = new MonotonicMap<>();

  @Override
  public boolean equals(Object o) {
    AbstractNode n = (AbstractNode) o;
    final boolean parentsEqual;
    if (n.parent == this.parent) {
      parentsEqual = true;
    } else if (n.parent == null) {
      parentsEqual = false;
    } else {
      parentsEqual = n.parent.equals(this.parent);
    }
    if (!parentsEqual) {
      return false;
    }
    return this.name.equals(n.name);
  }

  @Override
  public String getFullName() {
    final String ret;
    if (parent == null) {
      if (name == "") {
        ret = "[root node]";
      } else {
        throw new IllegalStateException(
            "can't have node with name and null parent!");
      }
    } else {
      String parentName = parent.getFullName();
      if (name == "") {
        throw new IllegalStateException(
            "non-root node had empty name.  Parent is " + parentName);
      }
      if (parentName.startsWith("[")) {
        ret = name;
      } else {
        ret = parentName + "." + name;
      }
    }
    return ret;
  }

  public AbstractNode(Node parent, String name) {
    this.parent = parent;
    this.name = name;
    if (parent != null) {
      if (name.length() == 0) {
        throw new IllegalArgumentException(
            "Zero length child name means bad news");
      }
      parent.put(this);
    }
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

  @Override
  public String toIndentedString(int level) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("\t");
    }
    sb.append(toString() + "\n");
    if (children != null) {
      for (Node n : children.values()) {
        sb.append(n.toIndentedString(level + 1));
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() + " '"
        + getFullName() + "']";
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