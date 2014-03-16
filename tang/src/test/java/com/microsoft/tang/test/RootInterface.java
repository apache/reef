package com.microsoft.tang.test;

/**
 * The interface for the root of the test object graph instantiated.
 */
public interface RootInterface {

  /**
   * @return true, if the object graph has been instantiated correctly, false otherwise.
   */
  public boolean isValid();

}
