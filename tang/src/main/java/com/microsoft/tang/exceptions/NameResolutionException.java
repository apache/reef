package com.microsoft.tang.exceptions;

/**
 * Thrown when Tang encounters an unknown (to the current classloader) class
 * or configuration option.  NameResolutionExceptions can only be encountered
 * if:
 * <ol>
 * <li> Tang is processing a configuration file from an external source </li>
 * <li> Classes / NamedParameters are passed to Tang in String form </li>
 * <li> Class objects are passed directly to Tang, but it is using a different
 *      classloader than the calling code.</li>
 * <li> Tang is processing Configurations using a ClassHierarchy produced by
 *      another process </li> 
 * </ol>
 */
public class NameResolutionException extends BindException {
  private static final long serialVersionUID = 1L;

  public NameResolutionException(String name, String longestPrefix) {
      super("Could not resolve " + name + ".  Search ended at prefix " + longestPrefix + " This can happen due to typos in class names that are passed as strings, or because Tang is configured to use a classloader other than the one that generated the class reference (check your classpath and the code that instantiated Tang)");
  }
}
