package com.microsoft.tang;

import com.microsoft.tang.types.Node;

public interface JavaClassHierarchy extends ClassHierarchy {
  /**
   * Look up a class object in this ClassHierarchy. Unlike the version that
   * takes a string in ClassHierarchy, this version does not throw
   * NameResolutionException.
   * 
   * The behavior of this method is undefined if the provided Class object
   * is not from the ClassLoader (or an ancestor of the ClassLoader)
   * associated with this JavaClassHierarchy.  By default, Tang uses the 
   * default runtime ClassLoader as its root ClassLoader, so static references
   * (expressions like getNode(Foo.class)) are safe.
   * 
   * @param c The class to be looked up in the class hierarchy.
   * @return The associated NamedParameterNode or ClassNode.
   */
  public Node getNode(Class<?> c);

  public Class<?> classForName(String name) throws ClassNotFoundException;
}
