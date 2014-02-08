/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.driver;

import com.microsoft.reef.task.Task;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaskConfigurationRoundTripTest {

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

    public final class DummyTask implements Task {
        @Override
        public byte[] call(byte[] memento) throws Exception {
            return null;
        }
    }

    private static Configuration configureMasterTask(int d, double eps) throws BindException {
        final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
        b.bindImplementation(Task.class, DummyTask.class);
        b.bindNamedParameter(Dimensionality.class, String.valueOf(d));
        b.bindNamedParameter(Eps.class, String.valueOf(eps));
        return b.build();
    }

    @SuppressWarnings("static-method")
    @Test
    public void testRoundTrip() throws BindException, InjectionException {
        final int d = 10;
        final double eps = 1e-5;
        final Configuration conf = configureMasterTask(d, eps);
        {
            final Injector i = Tang.Factory.getTang().newInjector(conf);

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }
        {
            final Injector i = Tang.Factory.getTang().newInjector(Utils.roundtrip(conf));

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }

    }

}
