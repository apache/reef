package com.microsoft.tang.implementation;

import java.util.Collection;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.ExternalConstructor;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicSet;

public class ConfigurationImpl implements Configuration {
  public final ConfigurationBuilderImpl builder;

  public ConfigurationImpl(ConfigurationBuilderImpl builder) {
    this.builder = builder;
  }

  @Override
  public String getNamedParameter(NamedParameterNode<?> np) {
    return builder.namedParameters.get(np);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ClassNode<? extends ExternalConstructor<T>> getBoundConstructor(
      ClassNode<T> cn) {
    return (ClassNode<? extends ExternalConstructor<T>>) builder.boundConstructors.get(cn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ClassNode<? extends T> getBoundImplementation(ClassNode<T> cn) {
    return (ClassNode<? extends T>) builder.boundImpls.get(cn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ConstructorDef<T> getLegacyConstructor(ClassNode<T> cn) {
    return (ConstructorDef<T>) builder.legacyConstructors.get(cn);
  }

  @Override
  public Collection<ClassNode<?>> getSingletons() {
    return new MonotonicSet<>(builder.singletons);
  }
  @Override
  public boolean isSingleton(Node cn) {
    return builder.singletons.contains(cn);
  }

  @Override
  public ConfigurationBuilder newBuilder() {
    return builder.clone();
  }

}