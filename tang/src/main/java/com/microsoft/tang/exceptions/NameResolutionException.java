package com.microsoft.tang.exceptions;

public class NameResolutionException extends Exception {
  private static final long serialVersionUID = 1L;

    public NameResolutionException(String name, String longestPrefix) {
        super("Could not resolve " + name + ".  Search ended at prefix " + longestPrefix);
    }
    public NameResolutionException(Exception e) {
      super(e);
  }
}
