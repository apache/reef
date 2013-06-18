package com.microsoft.tang.exceptions;

/**
 * This exception is thrown when Tang detects improper or inconsistent class
 * annotations.  This is a runtime exception because it denotes a problem that
 * existed during compilation.
 */
public class ClassHierarchyException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  public ClassHierarchyException(Throwable cause) {
    super(cause);
  }
  public ClassHierarchyException(String msg) {
    super(msg);
  }
  public ClassHierarchyException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
