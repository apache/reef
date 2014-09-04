package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

public class ListOfImplementations {

  private final List<ListInterface> theInstances;

  @Inject
  ListOfImplementations(@Parameter(TestConfiguration.ListOfInstances.class) final List<ListInterface> theInstances) {
    this.theInstances = theInstances;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ListOfImplementations that = (ListOfImplementations) o;

    if (!theInstances.equals(that.theInstances)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return theInstances.hashCode();
  }

  public boolean isValid() {
    return this.theInstances.size() == 2;
  }
}
