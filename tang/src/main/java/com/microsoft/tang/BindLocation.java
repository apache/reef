package com.microsoft.tang;

/**
 * This interface is used to track the source of configuration bindings.
 * 
 * This can be explicitly set (such as by configuration file parsers), or be
 * implicitly bound to the stack trace of bind() invocation.
 */
public interface BindLocation {
  /**
   * Implementations of BindLocation should override toString() so that it
   * returns a human readable representation of the source of the
   * configuration option in question. 
   * @return A (potentially multi-line) string that represents the stack
   *         trace, configuration file location, or other source of some
   *         configuration data.
   */
  public abstract String toString();
}
