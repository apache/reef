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
package org.apache.reef.runtime.multi.utils;

import org.apache.reef.runtime.multi.utils.avro.MultiRuntimeDefinition;
import org.apache.reef.runtime.multi.utils.avro.RuntimeDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by bshulman on 3/2/2016.
 */
public class MultiRuntimeDefinitionSerializerTests {
  @Test
  public void testSerailze() throws IOException {
    final MultiRuntimeDefinitionSerializer serializer = new MultiRuntimeDefinitionSerializer();
    final MultiRuntimeDefinition def = new MultiRuntimeDefinition();
    final RuntimeDefinition rd = new RuntimeDefinition();
    rd.setRuntimeName("default");
    rd.setSerializedConfiguration("config");
    def.setDefaultRuntimeName("default");
    def.setRuntimes(Arrays.asList(new RuntimeDefinition[]{rd}));
    final String serialized = serializer.toString(def);
    final MultiRuntimeDefinition fromStringdef = serializer.fromString(serialized);
    Assert.assertEquals(def, fromStringdef);
  }

  @Test(expected = IOException.class)
  public void testDeseerailzeThrowsOnBadData() throws IOException {
    final MultiRuntimeDefinitionSerializer serializer = new MultiRuntimeDefinitionSerializer();
    serializer.fromString("bad data");
  }
}
