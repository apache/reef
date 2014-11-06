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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class ObjectTreeTest {

  public static Configuration getConfiguration() throws BindException {
    return TestConfiguration.CONF
        .set(TestConfiguration.OPTIONAL_STRING, TestConfiguration.OPTIONAL_STRING_VALUE)
        .set(TestConfiguration.REQUIRED_STRING, TestConfiguration.REQUIRED_STRING_VALUE)
        .build();
  }

  /**
   * Configuration getter for TestConfigurationWithoutList.
   */
  public static Configuration getConfigurationWithoutList() throws BindException {
    // TODO: Remove this method after #192 is fixed
    return TestConfigurationWithoutList.CONF
        .set(TestConfigurationWithoutList.OPTIONAL_STRING, TestConfigurationWithoutList.OPTIONAL_STRING_VALUE)
        .set(TestConfigurationWithoutList.REQUIRED_STRING, TestConfigurationWithoutList.REQUIRED_STRING_VALUE)
        .build();
  }

  @Test
  public void testInstantiation() throws BindException, InjectionException {
    final RootInterface root = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    Assert.assertTrue("Object instantiation left us in an inconsistent state.", root.isValid());
  }

  @Test
  public void testTwoInstantiations() throws BindException, InjectionException {
    final RootInterface firstRoot = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    final RootInterface secondRoot = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    Assert.assertNotSame("Two instantiations of the object tree should not be the same", firstRoot, secondRoot);
    Assert.assertEquals("Two instantiations of the object tree should be equal", firstRoot, secondRoot);
  }
}
