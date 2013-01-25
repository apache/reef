package com.example;

import com.microsoft.tang.implementation.TypeHierarchy;

public class PrintTypeHierarchy {

  public static void main(String[] args) throws Exception {
    TypeHierarchy ns = new TypeHierarchy();
    for (String s : args) {
      ns.register(s);
    }
    System.out.print(ns.toPrettyString());
  }
}
