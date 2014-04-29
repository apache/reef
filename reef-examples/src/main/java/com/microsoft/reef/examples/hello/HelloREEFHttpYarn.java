package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: juwang
 * Date: 4/29/14
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class HelloREEFHttpYarn {

    private static final Logger LOG = Logger.getLogger(HelloReefYarn.class.getName());

    /**
     * Number of milliseconds to wait for the job to complete.
     */
    private static final int JOB_TIMEOUT = 300000; // 300 sec.

    /**
     * Start Hello REEF job. Runs method runHelloReef().
     *
     * @param args command line parameters.
     * @throws com.microsoft.tang.exceptions.BindException      configuration error.
     * @throws com.microsoft.tang.exceptions.InjectionException configuration error.
     */
    public static void main(final String[] args) throws BindException, InjectionException, IOException {

        final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();

        final LauncherStatus status = HelloREEFHttp.runHelloReef(runtimeConfiguration, JOB_TIMEOUT);
        LOG.log(Level.INFO, "REEF job completed: {0}", status);
    }
}
