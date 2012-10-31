package com.microsoft.tang.exceptions;

public class BindException extends Exception {
  private static final long serialVersionUID = 1L;
  public BindException(String msg, Throwable cause) {
    super(msg,cause);
  }
}
