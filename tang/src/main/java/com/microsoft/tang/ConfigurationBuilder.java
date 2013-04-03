package com.microsoft.tang;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.Node;

public interface ConfigurationBuilder {

  /**
   * Add all configuration parameters from the given Configuration object.
   * 
   * @param c
   */
  public void addConfiguration(final Configuration c) throws BindException;

  /**
   * @return a reference to the ClassHierarchy instance backing this
   *         ConfigurationBuilder. No copy is made, since ClassHierarchy objects
   *         are effectively immutable.
   */
  public ClassHierarchy getClassHierarchy();

  /**
   * Force Tang to treat the specified constructor as though it had an @Inject
   * annotation.
   * 
   * @param c
   *          The class the constructor instantiates.
   * @param args
   *          The arguments taken by the constructor, in declaration order.
   */
  void registerLegacyConstructor(ClassNode<?> cn, ClassNode<?>... args)
      throws BindException;

  void registerLegacyConstructor(String cn, String... args)
      throws BindException;

  void registerLegacyConstructor(ClassNode<?> c, ConstructorArg... args)
      throws BindException;

  /**
   * Bind classes to each other, based on their full class names.
   * 
   * @param iface
   * @param impl
   */
  public <T> void bind(String iface, String impl)
      throws BindException;

  void bind(Node key, Node value) throws BindException;

  public void bindSingleton(String iface) throws BindException;

  public void bindSingleton(ClassNode<?> iface) throws BindException;

  public <T> void bindConstructor(ClassNode<T> k,
      ClassNode<? extends ExternalConstructor<? extends T>> v)
      throws BindException;

  public String classPrettyDefaultString(String longName) throws BindException;

  public String classPrettyDescriptionString(String longName)
      throws BindException;

  public Configuration build();

  public <T> void bindSingletonImplementation(ClassNode<T> c,
      ClassNode<? extends T> d) throws BindException;

  public void bindSingletonImplementation(String inter, String impl)
      throws BindException;

}
