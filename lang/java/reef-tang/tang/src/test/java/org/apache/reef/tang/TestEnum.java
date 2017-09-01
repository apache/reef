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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestEnum {

  public enum FooEnum { BAR, BAZ }

  @NamedParameter(default_value = "BAR")
  private final class FooParam implements Name<FooEnum> { }

  protected Tang tang;

  @Before
  public void setUp() throws Exception {
    tang = Tang.Factory.getTang();
  }

  @Test
  public void testEnumDefault() throws InjectionException {
    final Configuration config = tang.newConfigurationBuilder().build();
    final Injector injector = tang.newInjector(config);
    Assert.assertEquals(FooEnum.BAR, injector.getNamedInstance(FooParam.class));
  }

  @Test
  public void testEnumFromString() throws InjectionException {
    final Configuration config = tang.newConfigurationBuilder()
        .bindNamedParameter(FooParam.class, "BAZ")
        .build();
    final Injector injector = tang.newInjector(config);
    Assert.assertEquals(FooEnum.BAZ, injector.getNamedInstance(FooParam.class));
  }
}
