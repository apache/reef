package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Returns tracking URI
 */
public class TrackingUriImpl implements TrackingUri {

    @NamedParameter(default_value = "8080")
    public class PortNumber implements Name<String>
    {}

    private String port;

    @Inject
    public TrackingUriImpl(@Parameter(PortNumber.class) String port) {
        this.port = port;
    }

    @Override
    public String GetUri() {
        try {
            return InetAddress.getLocalHost().getHostAddress() + ":" + port;
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return null;
        }
    }
}
