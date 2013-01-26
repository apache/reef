package com.microsoft.tang;

public interface Node extends Comparable<Node> {

  public String getName();
  public String getFullName();

  public boolean contains(String key);
  public Node get(String key);

  public String toIndentedString(int level);
  public String toString();
  
}