package com.microsoft.wake.impl;

import com.microsoft.wake.Identifier;

public class OpaqueLocalIdentifier implements Identifier{
  @Override
  public String toString() {
    return "local-opaque-id://" + this.toString();
  }
}
