package com.microsoft.tang.exceptions;

public class InjectionException extends Exception {
  private static final long serialVersionUID = 1L;
  public InjectionException(String msg, Throwable cause) {
    super(msg,cause);
  }
  public InjectionException(String msg) {
    super(msg);
  }
}
