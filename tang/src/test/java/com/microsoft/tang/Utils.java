package com.microsoft.tang;

import java.io.File;

public class Utils {

    public static Configuration roundtrip(final Configuration conf) {
        try {
            final File f = File.createTempFile("TANGConf-", ".conf");
            conf.writeConfigurationFile(f);
            final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
            b.addConfiguration(f);
            return b.build();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to roundtrip a TANG Configuration.", e);
        }
    }

}
