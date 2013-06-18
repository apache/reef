package com.microsoft.tang.exceptions;

/**
 * Thrown when an illegal or contradictory configuration option is encountered.
 * 
 * While binding configuration values and merging Configuration objects, Tang
 * checks each configuration option to make sure that it is correctly typed,
 * and that it does not override some other setting (even if the two settings
 * bind the same configuration option to the same value).  When a bad
 * configuration option is encountered, a BindException is thrown.
 *
 * @see NameResolutionException which covers the special case where an unknown
 *      configuration option or class is encountered.
 */
public class BindException extends Exception {
  private static final long serialVersionUID = 1L;
  public BindException(String msg, Throwable cause) {
    super(msg,cause);
  }
  public BindException(String msg) {
    super(msg);
  }
}
