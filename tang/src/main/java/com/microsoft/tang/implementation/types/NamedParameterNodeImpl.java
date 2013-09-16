package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;

public class NamedParameterNodeImpl<T> extends AbstractNode implements
    NamedParameterNode<T> {
  private final String fullArgName;
  private final String simpleArgName;
  private final String documentation;
  private final String shortName;
  private final String[] defaultInstanceAsStrings;
  private final boolean isSet;
  
  public NamedParameterNodeImpl(Node parent, String simpleName,
      String fullName, String fullArgName, String simpleArgName, boolean isSet,
      String documentation, String shortName, String[] defaultInstanceAsStrings) {
    super(parent, simpleName, fullName);
    this.fullArgName = fullArgName;
    this.simpleArgName = simpleArgName;
    this.isSet = isSet;
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
}