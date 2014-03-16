package com.microsoft.tang.test;


/**
 * The root of the object graph instantiated.
 */
public class RootImplementation implements RootInterface {
  @Override
  public boolean isValid() {
    return false;
  }
}
