package com.microsoft.tang.implementation;

import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;

public class NamedParameterNodeImpl<T> extends AbstractNode implements
    NamedParameterNode<T> {
  private final String fullName;
  private final String fullArgName;
  private final String simpleArgName;
  private final String documentation;
  private final String shortName;
  private final String defaultInstanceAsString;

  public NamedParameterNodeImpl(Node parent, String simpleName,
      String fullName, String fullArgName, String simpleArgName,
      String documentation, String shortName, String defaultInstanceAsString) {
    super(parent, simpleName);
    this.fullName = fullName;
    this.fullArgName = fullArgName;
    this.simpleArgName = simpleArgName;
    this.documentation = documentation;
    this.shortName = shortName;
    this.defaultInstanceAsString = defaultInstanceAsString;
  }

  @Override
  public String toString() {
    return getSimpleArgName() + " " + getName();
  }

  @Override
  public String getFullName() {
    return fullName;
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
  public String getDefaultInstanceAsString() {
    return defaultInstanceAsString;
  }
}