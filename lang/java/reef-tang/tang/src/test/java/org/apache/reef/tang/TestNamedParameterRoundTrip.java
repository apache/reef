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
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestNamedParameterRoundTrip {

  @Test
  public void testRoundTrip() throws BindException, InjectionException, IOException {
    final int d = 10;
    final double eps = 1e-5;
    final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
    b.bindNamedParameter(Dimensionality.class, String.valueOf(d));
    b.bindNamedParameter(Eps.class, String.valueOf(eps));
    final Configuration conf = b.build();

    final Injector i1 = Tang.Factory.getTang().newInjector(conf);

    final int readD1 = i1.getNamedInstance(Dimensionality.class).intValue();
    final double readEps1 = i1.getNamedInstance(Eps.class).doubleValue();

    assertEquals(eps, readEps1, 1e-12);
    assertEquals(d, readD1);

    final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();

    final JavaConfigurationBuilder roundTrip2 = Tang.Factory.getTang().newConfigurationBuilder();
    avroSerializer.configurationBuilderFromString(avroSerializer.toString(conf), roundTrip2);
    final Injector i2 = Tang.Factory.getTang().newInjector(roundTrip2.build());

    final int readD2 = i2.getNamedInstance(Dimensionality.class).intValue();
    final double readEps2 = i2.getNamedInstance(Eps.class).doubleValue();

    assertEquals(eps, readEps2, 1e-12);
    assertEquals(d, readD2);


    final Injector parent3 =
        Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
    final Injector i3 = parent3.forkInjector(conf);

    final int readD3 = i3.getNamedInstance(Dimensionality.class).intValue();
    final double readEps3 = i3.getNamedInstance(Eps.class).doubleValue();

    assertEquals(eps, readEps3, 1e-12);
    assertEquals(d, readD3);


    final Injector parent4 =
        Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
    final JavaConfigurationBuilder roundTrip4 = Tang.Factory.getTang().newConfigurationBuilder();
    avroSerializer.configurationBuilderFromString(avroSerializer.toString(conf), roundTrip4);
    final Injector i4 = parent4.forkInjector(roundTrip4.build());

    final int readD4 = i4.getNamedInstance(Dimensionality.class).intValue();
    final double readEps4 = i4.getNamedInstance(Eps.class).doubleValue();

    assertEquals(eps, readEps4, 1e-12);
    assertEquals(d, readD4);
  }

  @NamedParameter()
  public final class Dimensionality implements Name<Integer> {
    // Intentionally Empty
  }

  /**
   * Break criterion for the optimizer. If the progress in mean loss between
   * two iterations is less than this, the optimization stops.
   */
  @NamedParameter()
  public final class Eps implements Name<Double> {
    // Intentionally Empty
  }

}
