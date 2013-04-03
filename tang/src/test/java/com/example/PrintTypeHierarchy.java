package com.example;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;

public class PrintTypeHierarchy {

  public static void main(String[] args) throws Exception {
    ClassHierarchy ns = new ClassHierarchyImpl();
    for (String s : args) {
      ns.getNode(s);
    }
    System.out.print(ns.toPrettyString());
  }
}
