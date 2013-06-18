package com.microsoft.tang.exceptions;

/**
 * Thrown when an injection fails.  Injections commonly fail for two reasons.
 * The first is that the InjectionPlan that Tang produced is ambiguous or
 * infeasible.  The second is that a constructor invoked by Tang itself threw
 * an exception.
 * 
 * A third, less common issue arises when constructors obtain a handle to the
 * Tang Injector that created them, and then attempt to modify its state. 
 * Doing so is illegal, and results in a runtime exception that Tang converts
 * into an InjectionException.  Code involved in such exceptions is typically
 * attempting to perform cyclic object injections, and should use an
 * InjectionFuture instead. 
 */
public class InjectionException extends Exception {
  private static final long serialVersionUID = 1L;
  public InjectionException(String msg, Throwable cause) {
    super(msg,cause);
  }
  public InjectionException(String msg) {
    super(msg);
  }
}
