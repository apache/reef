/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.test;


import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The root of the object graph instantiated.
 */
final class RootImplementation implements RootInterface {

  private final String requiredString;
  private final String optionalString;
  private final UnitClass unit;
  private final Handler<String> stringHandler;
  private final Handler<Integer> integerHandler;
  private final AnInterface anInterface;
  private final int anInt;
  private final double aDouble;
  private final InjectableClass injectableClass;
  private final SetOfImplementations setOfImplementations;
  private final SetOfBaseTypes setOfBaseTypes;
  private final ListOfImplementations listOfImplementations;
  private final ListOfBaseTypes listOfBaseTypes;
  private final CyclicDependency cyclicDependency;

  @Inject
  public RootImplementation(@Parameter(TestConfiguration.RequiredString.class) final String requiredString,
                            @Parameter(TestConfiguration.OptionalString.class) final String optionalString,
                            @Parameter(TestConfiguration.StringHandler.class) final Handler<String> stringHandler,
                            @Parameter(TestConfiguration.IntegerHandler.class) final Handler<Integer> integerHandler,
                            @Parameter(TestConfiguration.NamedParameterInteger.class) final int anInt,
                            @Parameter(TestConfiguration.NamedParameterDouble.class) double aDouble,
                            final UnitClass unit,
                            final AnInterface anInterface,
                            final InjectableClass injectableClass,
                            final SetOfImplementations setOfImplementations,
                            final SetOfBaseTypes setOfBaseTypes,
                            final ListOfImplementations listOfImplementations,
                            final ListOfBaseTypes listOfBaseTypes,
                            CyclicDependency cyclicDependency) {
    this.requiredString = requiredString;
    this.optionalString = optionalString;
    this.unit = unit;
    this.stringHandler = stringHandler;
    this.integerHandler = integerHandler;
    this.anInterface = anInterface;
    this.anInt = anInt;
    this.aDouble = aDouble;
    this.injectableClass = injectableClass;
    this.setOfImplementations = setOfImplementations;
    this.setOfBaseTypes = setOfBaseTypes;
    this.listOfImplementations = listOfImplementations;
    this.listOfBaseTypes = listOfBaseTypes;
    this.cyclicDependency = cyclicDependency;
  }

  @Override
  public boolean isValid() {
    if (!this.setOfImplementations.isValid()) {
      return false;
    }
    if (!this.listOfImplementations.isValid()) {
      return false;
    }
    if (!this.requiredString.equals(TestConfiguration.REQUIRED_STRING_VALUE)) {
      return false;
    }

    if (!this.optionalString.equals(TestConfiguration.OPTIONAL_STRING_VALUE)) {
      return false;
    }

    this.integerHandler.process(3);
    this.stringHandler.process("three");
    if (this.unit.getIntValue() != 3) {
      return false;
    }
    if (!this.unit.getStringValue().equals("three")) {
      return false;
    }
    if (this.anInterface == null) {
      return false;
    }

    if (this.aDouble != TestConfiguration.NAMED_PARAMETER_DOUBLE_VALUE) {
      return false;
    }
    if (this.anInt != TestConfiguration.NAMED_PARAMETER_INTEGER_VALUE) {
      return false;
    }

    return true;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RootImplementation that = (RootImplementation) o;

    if (Double.compare(that.aDouble, aDouble) != 0) return false;
    if (anInt != that.anInt) return false;
    if (anInterface != null ? !anInterface.equals(that.anInterface) : that.anInterface != null) return false;
    if (integerHandler != null ? !integerHandler.equals(that.integerHandler) : that.integerHandler != null)
      return false;
    if (optionalString != null ? !optionalString.equals(that.optionalString) : that.optionalString != null)
      return false;
    if (requiredString != null ? !requiredString.equals(that.requiredString) : that.requiredString != null)
      return false;
    if (stringHandler != null ? !stringHandler.equals(that.stringHandler) : that.stringHandler != null) return false;
    if (unit != null ? !unit.equals(that.unit) : that.unit != null) return false;
    if (injectableClass != null ? !injectableClass.equals(that.injectableClass) : that.injectableClass != null)
      return false;
    if (setOfImplementations != null ? !setOfImplementations.equals(that.setOfImplementations) : that.setOfImplementations != null)
      return false;
    if (setOfBaseTypes != null ? !setOfBaseTypes.equals(that.setOfBaseTypes) : that.setOfBaseTypes != null)
      return false;
    if (listOfImplementations != null ? !listOfImplementations.equals(that.listOfImplementations) : that
        .listOfImplementations != null)
      return false;
    if (listOfBaseTypes != null ? !listOfBaseTypes.equals(that.listOfBaseTypes) : that.listOfBaseTypes != null)
      return false;
    if (cyclicDependency != null ? !cyclicDependency.equals(that.cyclicDependency) : that.cyclicDependency != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = requiredString != null ? requiredString.hashCode() : 0;
    result = 31 * result + (optionalString != null ? optionalString.hashCode() : 0);
    result = 31 * result + (unit != null ? unit.hashCode() : 0);
    result = 31 * result + (stringHandler != null ? stringHandler.hashCode() : 0);
    result = 31 * result + (integerHandler != null ? integerHandler.hashCode() : 0);
    result = 31 * result + (anInterface != null ? anInterface.hashCode() : 0);
    result = 31 * result + anInt;
    temp = Double.doubleToLongBits(aDouble);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
