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

import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.*;
import java.net.UnknownHostException;
import java.net.InetAddress;

/**
 * Tracking Uri test
 */
public class TestTrackingUri {
    /**
     * Get default Tracking Uri
     * @throws InjectionException
     * @throws UnknownHostException
     */
    @Test
    public void DefaultTrackingUriTest () throws InjectionException, UnknownHostException {
        String trackingId = InetAddress.getLocalHost().getHostAddress() + ":8080";
        String uri = Tang.Factory.getTang().newInjector().getInstance(TrackingUri.class).GetUri();
        Assert.assertEquals(uri, trackingId);
    }

    /**
     * Get Tracking Uri with user define port number
     * @throws InjectionException
     * @throws UnknownHostException
     * @throws BindException
     */
    @Test
    public void TrackingUriTest () throws InjectionException, UnknownHostException, BindException {
        String trackingId = InetAddress.getLocalHost().getHostAddress() + ":8088";
        JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
        cb.bindNamedParameter(TrackingUriImpl.PortNumber.class, "8088");
        String uri = Tang.Factory.getTang().newInjector(cb.build()).getInstance(TrackingUri.class).GetUri();
        Assert.assertEquals(uri, trackingId);
    }
}
