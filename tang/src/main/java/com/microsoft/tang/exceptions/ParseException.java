package com.microsoft.tang.exceptions;
/**
 * Thrown when a string fails to parse as the requested type.
 */
public class ParseException extends BindException {
  private static final long serialVersionUID = 1L;
  public ParseException(String msg, Throwable cause) {
    super(msg,cause);
  }
  public ParseException(String msg) {
    super(msg);
  }
}
