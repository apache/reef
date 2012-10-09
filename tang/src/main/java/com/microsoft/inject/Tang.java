package com.microsoft.inject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.microsoft.inject.Namespace.ClassNode;
import com.microsoft.inject.Namespace.NamedParameterNode;
import com.microsoft.inject.Namespace.Node;

public class Tang {
    private final Configuration conf;
    private final Namespace namespace;
    private final Map<Node, Object> boundValues
    	= new HashMap<Node, Object>();
    public Tang(Namespace namespace) {
    	this.conf = null;
    	this.namespace = namespace;
    }
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
    public void setDefaultImpl(Class<?> c, Class<?> d) {
    	if(!c.isAssignableFrom(d)) {
    		throw new ClassCastException(d.getName() + " does not extend or implement " + c.getName());
    	}
    	Node n = namespace.getNode(c);
    	if(n instanceof ClassNode) {
        	boundValues.put(n, d);
    	} else {
    		throw new IllegalArgumentException("Detected type mismatch.  Expected ClassNode, but namespace contains a " + n);
    	}
    }
    public void setNamedParameter(String name, Object o) {
    	Node n = namespace.getNode(name);
    	if(n == null) {
    		throw new IllegalArgumentException("Unknown NamedParameter: " + name);
    	}
    	if(n instanceof NamedParameterNode) {
    		NamedParameterNode np = (NamedParameterNode)n;
    		if(np.argClass.isAssignableFrom(o.getClass())) {
    			boundValues.put(n, o);
    		} else {
    			throw new ClassCastException("Cannot cast from " + o.getClass()+ " to " + np.argClass);
    		}
    	} else {
    		throw new IllegalArgumentException("Detected type mismatch when setting named parameter " + name + "  Expected NamedParameterNode, but namespace contains a " + n);
    	}
    }
}
