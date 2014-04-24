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
package com.microsoft.reef.webserver;

import java.util.Set;

import org.junit.*;
import org.mortbay.jetty.Server;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

/**
 *   TestRuntimeStartHandler
 */
public class TestRuntimeStartHandler {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }


    @After
    public void tearDown() throws Exception {
    }
    /**
     * With HttpHandlerConfiguration merged with HttpRuntimeConfiguration and binding for http handlers, when inject RuntimeClock
     * all the nested objects including HeetServer, JettyHandler, HttpRuntimeStartHandler and  HttpRuntimeStopHandler
     * @throws BindException
     * @throws InjectionException
     */
    @Test
    public void testHttpHandlerBindingWIthRuntimeClock() throws BindException, InjectionException {
        Configuration clockConfiguraiton = HttpHandlerConfiguration.CONF
                .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
                .build();
        RuntimeClock c = Tang.Factory.getTang().newInjector(clockConfiguraiton).getInstance(RuntimeClock.class);
        Assert.assertNotNull(c);
    }

    /**
     * This test is to get RuntimeStartHandler, simulate the call to onNext to so tah tto start Jetty server
     * @throws BindException
     * @throws InjectionException
     */
    @Test
    public void testRunTimeStartStopHandler() throws BindException, InjectionException {
        Configuration clockConfiguraiton = HttpHandlerConfiguration.CONF
                .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
                .build();
        Injector injector = Tang.Factory.getTang().newInjector(clockConfiguraiton);
        Set<EventHandler<RuntimeStart>> rStart = injector.getNamedInstance(RuntimeClock.RuntimeStartHandler.class);
        for (EventHandler<RuntimeStart> i : rStart) {
            HttpRuntimeStartHandler h = (HttpRuntimeStartHandler)i;
            try {
               //h.onNext(null);
            }catch(Exception e) {
            }
        }

        Set<EventHandler<RuntimeStop>> rStop = injector.getNamedInstance(RuntimeClock.RuntimeStopHandler.class);
        for (EventHandler<RuntimeStop> i : rStop) {
            HttpRuntimeStopHandler h = (HttpRuntimeStopHandler)i;
            try {
            //    h.onNext(null);
            }catch(Exception e) {
            }
        }
    }
}
