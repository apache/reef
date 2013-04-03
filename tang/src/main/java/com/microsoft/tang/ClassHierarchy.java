package com.microsoft.tang;

import java.util.Set;
 
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.ParseException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.NamedParameterNode;
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

  public Set<String> getRegisteredClassNames();
  
  /**
   * TODO: Fix up output of TypeHierarchy!
   * 
   * @return
   */
  public String toPrettyString();

  boolean isImplementation(ClassNode<?> inter, ClassNode<?> impl);

  public ClassHierarchy merge(ClassHierarchy ch);

  public <T> T parse(NamedParameterNode<T> name, String value) throws ParseException;

  public <T> T parseDefaultValue(NamedParameterNode<T> name) throws ParseException;

}