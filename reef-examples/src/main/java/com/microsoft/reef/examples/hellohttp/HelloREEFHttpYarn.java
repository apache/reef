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
package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HelloREEFHttp for running on Yarn
 */
public class HelloREEFHttpYarn {

    private static final Logger LOG = Logger.getLogger(HelloREEFHttpYarn.class.getName());

    /**
     * Start Hello REEF job. Runs method runHelloReef().
     *
     * @param args command line parameters.
     * @throws com.microsoft.tang.exceptions.BindException      configuration error.
     * @throws com.microsoft.tang.exceptions.InjectionException configuration error.
     */
    public static void main(final String[] args) throws BindException, InjectionException, IOException {

        final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();

        final LauncherStatus status = HelloREEFHttp.runHelloReef(runtimeConfiguration, HelloREEFHttp.JOB_TIMEOUT);
        LOG.log(Level.INFO, "REEF job completed: {0}", status);
    }
}
