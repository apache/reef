package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Set;

final class SetOfImplementations {

  private final Set<SetInterface> theInstances;

  @Inject
  SetOfImplementations(@Parameter(TestConfiguration.SetOfInstances.class) final Set<SetInterface> theInstances) {
    this.theInstances = theInstances;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SetOfImplementations that = (SetOfImplementations) o;

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
