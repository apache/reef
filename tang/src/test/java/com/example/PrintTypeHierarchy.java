package com.example;

import javax.inject.Inject;

import com.microsoft.tang.Tang;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import com.microsoft.tang.util.walk.Walk;
import com.microsoft.tang.util.walk.GraphVisitor;
import com.microsoft.tang.util.walk.GraphVisitorGraphviz;

public class PrintTypeHierarchy {

  @NamedParameter(default_value="999", doc="Test parameter", short_name="id")
  class Id implements Name<Integer> {}

  private final int mId;

  @Inject
  public PrintTypeHierarchy(@Parameter(PrintTypeHierarchy.Id.class) int aId) {
    this.mId = aId;
  }

  public static void main(String[] args) throws BindException, InjectionException {

    final Tang tang = Tang.Factory.getTang();
    final ConfigurationBuilder confBuilder = tang.newConfigurationBuilder();
    Configuration config = confBuilder.build();

    final GraphVisitor visitor = new GraphVisitorGraphviz(config);
    Walk.preorder(visitor, config);
    System.out.println(visitor);
  }
}
