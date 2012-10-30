package com.microsoft.tang.exceptions;

public class InjectionException extends Exception {
  private static final long serialVersionUID = 1L;

    public InjectionException() {
    }

    public InjectionException(String string) {
        super(string);
    }

    public InjectionException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public InjectionException(Throwable thrwbl) {
        super(thrwbl);
    }

    public InjectionException(String string, Throwable thrwbl, boolean bln, boolean bln1) {
        super(string, thrwbl, bln, bln1);
    }

}
