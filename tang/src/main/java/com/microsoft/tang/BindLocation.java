package com.microsoft.tang;

/**
 * This interface is used to track the source of configuration bindings.
 * 
 * This can be explicitly set (such as by configuration file parsers), or be
 * implicitly bound to the stack trace of bind() invocation.
 */
public interface BindLocation {
  public abstract String toString();
}
