package com.microsoft.inject;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;

public class Tang {
    private final Configuration conf;
    private final Namespace namespace;

    public Tang(Configuration conf) {
        this.conf = conf;
        this.namespace = new Namespace();

        Iterator<String> it = this.conf.getKeys();

        while(it.hasNext()) {
            String key = it.next();
            String value = this.conf.getString(key);
            
            if(key.equals("require")) {
                try {
                    namespace.registerClass(Class.forName(value));
                } catch (ClassNotFoundException e) {
                    // print error message + exit.
                }
            }
            
        }
    }
}
