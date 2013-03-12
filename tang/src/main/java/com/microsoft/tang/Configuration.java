package com.microsoft.tang;

import java.util.Collection;
import java.util.Set;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;

/**
 * TANG Configuration object.
 * 
 * Tang Configuration objects are immutable and constructed via
 * ConfigurationBuilders.
 * 
 * @author sears
 */
public interface Configuration {

  public ConfigurationBuilder newBuilder();
  
  public String getNamedParameter(NamedParameterNode<?> np);

  public <T> ClassNode<? extends ExternalConstructor<T>> getBoundConstructor(ClassNode<T> cn);

  public <T> ClassNode<? extends T> getBoundImplementation(ClassNode<T> cn);
  
  public <T> ConstructorDef<T> getLegacyConstructor(ClassNode<T> cn);
  
  public Collection<ClassNode<?>> getSingletons();

  public boolean isSingleton(Node n);

  Set<ClassNode<?>> getBoundImplementations();

  Set<ClassNode<?>> getBoundConstructors();

  Set<NamedParameterNode<?>> getNamedParameters();

  Set<ClassNode<?>> getLegacyConstructors();

  public ClassHierarchy getClassHierarchy();
}