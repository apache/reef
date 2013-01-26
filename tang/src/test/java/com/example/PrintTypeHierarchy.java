package com.example;

import com.microsoft.tang.implementation.ClassHierarchyImpl;

public class PrintTypeHierarchy {

  public static void main(String[] args) throws Exception {
    ClassHierarchyImpl ns = new ClassHierarchyImpl();
    for (String s : args) {
      ns.register(s);
    }
    System.out.print(ns.toPrettyString());
  }
}
