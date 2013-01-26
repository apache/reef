package com.microsoft.tang;

import java.util.Collection;

import com.microsoft.tang.exceptions.BindException;

public interface ClassHierarchy {

  public Node register(String s) throws BindException;

  public Collection<String> getShortNames();

  public String resolveShortName(String shortName);

  /**
   * TODO: Fix up output of TypeHierarchy!
   * 
   * @return
   */
  public String toPrettyString();

}