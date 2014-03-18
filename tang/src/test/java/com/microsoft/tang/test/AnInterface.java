package com.microsoft.tang.test;

import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * An interface with a default implementation
 */
@DefaultImplementation(AnInterfaceImplementation.class)
interface AnInterface {
  void aMethod();
}
