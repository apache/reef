/*
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

import org.apache.reef.tang.annotations.Unit;

import javax.inject.Inject;

/**
 * A test user for the @Unit annotation.
 */
@Unit
final class UnitClass {
  private String stringValue;
  private int intValue;

  @Inject
  UnitClass() {

  }

  public String getStringValue() {
    return stringValue;
  }

  public int getIntValue() {
    return intValue;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final UnitClass unitClass = (UnitClass) o;

    if (intValue != unitClass.intValue) {
      return false;
    }
    if (stringValue != null ? !stringValue.equals(unitClass.stringValue) : unitClass.stringValue != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = stringValue != null ? stringValue.hashCode() : 0;
    result = 31 * result + intValue;
    return result;
  }

  final class IntegerHandler implements Handler<Integer> {
    private final int foo = 42;

    @Override
    public void process(final Integer value) {
      UnitClass.this.intValue = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final IntegerHandler that = (IntegerHandler) o;

      if (foo != that.foo) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return foo;
    }
  }

  final class StringHandler implements Handler<String> {
    private final int bar = -42;

    @Override
    public void process(final String value) {
      UnitClass.this.stringValue = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final StringHandler that = (StringHandler) o;

      if (bar != that.bar) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return bar;
    }
  }


}
