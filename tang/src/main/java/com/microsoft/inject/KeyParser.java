package com.microsoft.inject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Given a string, use reflection and prefix macros to resolve to the
 * appropriate Java class.
 * 
 * @author sears
 * 
 */
public class KeyParser {
    private final Map<String, Class<?>> macros;

    public KeyParser(Map<String, Class<?>> macros) {
        // TODO: Get these via reflection, by looking for ConfigurationNamespace annotations.
        this.macros = macros;
    }

    Key parse(String key) {
        // Split the key on .
        String[] toks = key.split("\\.");
        List<String> suffix = new LinkedList<String>();
        for (String t : toks) {
            suffix.add(t);
        }

        // Parse the prefix.  TODO: Handle inner classes...
        StringBuilder prefix = new StringBuilder(suffix.remove(0));
        Class<?> c = null;
        while (suffix.size() > 0) {
            prefix.append("." + suffix.remove(0));
            String prefix_s = prefix.toString();
            final boolean isMacro;
            boolean isClass;
            
            if(macros.containsKey(prefix_s)) {
                isMacro = true;
                c = macros.get(prefix_s);
            } else {
                isMacro = false;
            }
            try {
                c = Class.forName(prefix_s);
                isClass = true;
            } catch (ClassNotFoundException e) {
                isClass = false;
            }
            if (isClass && isMacro) {
                throw new IllegalStateException(
                        "Detected macro name that is also a class: " + prefix_s);
            }
            if(c != null) {
                break;
            }
        }
        if(c == null) { return null; }
        // We have parsed the prefix and resolved it to a class.  Now, we need to parse the suffix.
        
        // Option 1: The suffix is a named parameter.
        
        // Option 2: The suffix is a field name.
        
        
        return null;
    }
}
