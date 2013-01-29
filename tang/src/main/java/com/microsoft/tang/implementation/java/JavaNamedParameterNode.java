package com.microsoft.tang.implementation.java;

import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.AbstractNode;
import com.microsoft.tang.util.ReflectionUtilities;

class JavaNamedParameterNode<T> extends AbstractNode implements
    NamedParameterNode<T> {
  private final String fullName;
  private final String fullArgName;
  private final String simpleArgName;
  private final String documentation;
  private final String shortName;
  private final String defaultInstanceAsString;

  JavaNamedParameterNode(Node parent, Class<? extends Name<T>> clazz,
      Class<T> argClass) throws BindException {
    super(parent, ReflectionUtilities.getSimpleName(clazz));
    this.fullName = ReflectionUtilities.getFullName(clazz);
    NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);
    this.fullArgName = ReflectionUtilities.getFullName(argClass);
    this.simpleArgName = ReflectionUtilities.getSimpleName(argClass);
    if (namedParameter == null
        || namedParameter.default_value().length() == 0) {
      this.defaultInstanceAsString = null;
    } else {
      try {
        this.defaultInstanceAsString = namedParameter.default_value();
      } catch (UnsupportedOperationException e) {
        throw new BindException("Could not register NamedParameterNode for "
            + clazz.getName() + ".  Default value "
            + namedParameter.default_value() + " failed to parse.", e);
      }
    }
    if (namedParameter != null) {
      this.documentation = namedParameter.doc();
      if (namedParameter.short_name() != null
          && namedParameter.short_name().length() == 0) {
        this.shortName = null;
      } else {
        this.shortName = namedParameter.short_name();
      }
    } else {
      this.documentation = "";
      this.shortName = null;
    }
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