package com.microsoft.tang;

import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.Node;

/**
 * ClassHierarchy objects store information about the interfaces
 * and implementations that are available in a particular runtime
 * environment.
 * 
 * When Tang is running inside the same environment as the injected
 * objects, ClassHierarchy is simply a read-only representation of
 * information that is made available via language reflection.
 * 
 * If Tang is set up to perform remote injection, then the ClassHierarchy
 * it runs against is backed by a flat file, or other summary of the
 * libraries that will be available during injection.
 *
 */
public interface ClassHierarchy {
  /**
   * Lookup a node in this class hierarchy.
   * 
   * @param fullName The full name of the class that will be looked up.
   * @return A non-null reference to a ClassNode or a NamedParameterNode.
   * @throws NameResolutionException If the class is not found.
   */
  public Node getNode(String fullName) throws NameResolutionException;

  /**
   * @return true if impl is a subclass of inter.
   */
  public boolean isImplementation(ClassNode<?> inter, ClassNode<?> impl);

  public ClassHierarchy merge(ClassHierarchy ch);

  /**
   * Return a reference to the root of the ClassHierarchy.  This is needed by
   * serialization routines (which have to iterate over the ClassHierarchy's
   * state).  Unfortunately, due to limitations of Java, JavaClassHierarchy
   * objects need to be built lazily as callers request classes by name.
   * There is no way to provide an interface that enumerates known claases
   * without exposing this fact; therefore, the Node returned by this method
   * can change over time.
   * 
   * Normal callers (all use cases except ClassHierarchy serialization)
   * should use getNode(String) to lookup classes, since getNamespace() can
   * not support lazy loading of unknown classes.
   * 
   * @return a reference to the root node of this ClassHierarchy.
   */
  Node getNamespace();

}