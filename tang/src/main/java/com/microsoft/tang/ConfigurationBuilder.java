package com.microsoft.tang;

import java.util.Collection;

import com.microsoft.tang.exceptions.BindException;


public interface ConfigurationBuilder {

  /**
   * Add all configuration parameters from the given Configuration object.
   * 
   * @param c
   */
  public void addConfiguration(final Configuration c) throws BindException;

  /**
   * Ask Tang to register a class. This does not create any new bindings, but
   * has a number important side effects.
   * 
   * In particular, it causes any @Namespace and @NamedParameter annotations to
   * be processed, which, in turn makes the Namespace available to configuration
   * files, and makes any short_names defined by the @NamedParameter annotations
   * to become available to command line parsers.
   * 
   * When registering a class, Tang transitively registers any enclosing and
   * enclosed classes as well. So, if you have one class with that contains many
   * \@NamedParameter classes, you only need to call register on the enclosing
   * class.
   * 
   * In addition to registering the class, this method also checks to make sure
   * the class is consistent with itself and the rest of the Tang classes that
   * have been registered.
   */
  public void register(String c) throws BindException;

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
   * @throws ClassNotFoundException
   */
  public <T> void bind(String iface, String impl)
      throws ClassNotFoundException, BindException;

  public void bindSingleton(String iface) throws BindException;

  public Collection<String> getShortNames();

  public String resolveShortName(String shortName) throws BindException;

  public String classPrettyDefaultString(String longName) throws BindException;

  public String classPrettyDescriptionString(String longName)
  throws BindException;

  public Configuration build();

}
