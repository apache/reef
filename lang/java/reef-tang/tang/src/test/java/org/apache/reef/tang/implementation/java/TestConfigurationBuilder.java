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
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Inject;

/**
 * TestConfigurationBuilder.
 */
public class TestConfigurationBuilder {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void nullStringValueTest() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("The value null set to the named parameter is illegal: class " +
        "org.apache.reef.tang.implementation.java.TestConfigurationBuilder$NamedParameterNoDefault$NamedString");

    Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NamedParameterNoDefault.NamedString.class, (String) null)
        .build();
  }

  static class NamedParameterNoDefault {
    private final String str;

    @Inject
    NamedParameterNoDefault(@Parameter(NamedString.class) final String str) {
      this.str = str;
    }

    @NamedParameter()
    class NamedString implements Name<String> {
    }
  }
}
