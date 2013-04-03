package com.microsoft.tang.types;

import java.util.Collection;

public interface Node extends Comparable<Node> {

  public String getName();
  public String getFullName();

  public boolean contains(String key);
  public Node get(String key);
  public Collection<Node> getChildren();
  public Node getParent();
  public String toString();
  public void put(Node n);
  
}