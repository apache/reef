/**
 * Copyright (C) 2014 Microsoft Corporation
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

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.microsoft.reef.runtime.yarn.driver.TrackingURLProvider;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

/**
 * Http TrackingURLProvider
 */
public class HttpTrackingURLProvider implements TrackingURLProvider {
    private static final Logger LOG = Logger.getLogger(HttpTrackingURLProvider.class.getName());
    private final String port;

    @NamedParameter(default_value = "8080")
    public class PortNumber implements Name<String>
    {}

    @Inject
    public HttpTrackingURLProvider(@Parameter(PortNumber.class) String port) {
        this.port = port;
    }

    @Override
    public String getTrackingUrl() {
        try {
            return InetAddress.getLocalHost().getHostAddress() + ":" + port;
        } catch (UnknownHostException e) {
            LOG.log(Level.WARNING, "Cannot get host address.", e);
            throw new RuntimeException("Cannot get host address.", e);
        }
    }
}
