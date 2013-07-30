package com.microsoft.tang;

import java.util.Collection;
import java.util.Set;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;

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
  /*
   * @return the external constructor that cn has been explicitly bound to, or null.
   */
  public <T> ClassNode<? extends ExternalConstructor<T>> getBoundConstructor(ClassNode<T> cn);
  /*
   * @return the implementation that cn has been explicitly bound to, or null.
   */
  public <T> ClassNode<? extends T> getBoundImplementation(ClassNode<T> cn);
  /**
   * TODO Should this return a set of ConstructorDefs instead?
   */
  public <T> ConstructorDef<T> getLegacyConstructor(ClassNode<T> cn);
  
  @Deprecated
  public Collection<ClassNode<?>> getSingletons();

  Set<ClassNode<?>> getBoundImplementations();

  Set<ClassNode<?>> getBoundConstructors();

  Set<NamedParameterNode<?>> getNamedParameters();

  Set<ClassNode<?>> getLegacyConstructors();

  public ClassHierarchy getClassHierarchy();
}