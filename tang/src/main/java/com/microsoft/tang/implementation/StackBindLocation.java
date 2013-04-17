package com.microsoft.tang.implementation;

import java.util.Arrays;

import com.microsoft.tang.BindLocation;

public class StackBindLocation implements BindLocation {
  final StackTraceElement[] stack;
  public StackBindLocation() {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    if(stack.length != 0) {
      this.stack = Arrays.copyOfRange(stack, 1, stack.length);
    } else {
      this.stack = new StackTraceElement[0];
    }
  }
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("[\n");
    for(StackTraceElement e: stack) {
      sb.append(e.toString() + "\n");
    }
    sb.append("]\n");
    return sb.toString();
  }
}
