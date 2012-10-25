package com.example;

import com.microsoft.tang.ReflectionUtilities;
import com.microsoft.tang.TypeHierarchy;

public class PrintTypeHierarchy {

  public static void main(String[] args) throws Exception {
    TypeHierarchy ns = new TypeHierarchy();
    for (String s : args) {
      ns.register(ReflectionUtilities.classForName(s));
    }
    System.out.print(ns.toPrettyString());
    ns.writeJson(System.out);
  }
}
