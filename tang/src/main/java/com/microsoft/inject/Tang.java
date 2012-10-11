package com.microsoft.inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.microsoft.inject.Namespace.ClassNode;
import com.microsoft.inject.Namespace.ConstructorArg;
import com.microsoft.inject.Namespace.ConstructorDef;
import com.microsoft.inject.Namespace.NamedParameterNode;
import com.microsoft.inject.Namespace.Node;

public class Tang {
  private final Configuration conf;
  private final Namespace namespace;
  private final Map<Node, Object> boundValues = new HashMap<Node, Object>();

  public Tang(Namespace namespace) {
    this.conf = null;
    this.namespace = namespace;
  }

  public Tang(Configuration conf) {
    this.conf = conf;
    this.namespace = new Namespace();

    Iterator<String> it = this.conf.getKeys();

    while (it.hasNext()) {
      String key = it.next();
      String value = this.conf.getString(key);

      if (key.equals("require")) {
        try {
          namespace.registerClass(Class.forName(value));
        } catch (ClassNotFoundException e) {
          // print error message + exit.
        }
      }

    }
  }

  public void setDefaultImpl(Class<?> c, Class<?> d) {
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.getNode(c);
    if (n instanceof ClassNode) {
      boundValues.put(n, d);
    } else {
      throw new IllegalArgumentException(
          "Detected type mismatch.  Expected ClassNode, but namespace contains a "
              + n);
    }
  }

  public void setNamedParameter(String name, Object o) {
    // TODO: Better error messages. List first thing in path that doesn't
    // resolve.
    Node n = namespace.getNode(name);
    if (n == null) {
      throw new IllegalArgumentException("Unknown NamedParameter: " + name);
    }
    if (n instanceof NamedParameterNode) {
      NamedParameterNode np = (NamedParameterNode) n;
      if (ReflectionUtilities.isCoercable(np.argClass, o.getClass())) {
        boundValues.put(n, o);
      } else {
        throw new ClassCastException("Cannot cast from " + o.getClass()
            + " to " + np.argClass);
      }
    } else {
      throw new IllegalArgumentException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + n);
    }
  }

  public Object getInstance(Class<?> clazz) throws Exception { //XXX
    Node n = namespace.getNode(clazz);
    if (n instanceof ClassNode) {
      Class<?> c = (Class<?>) boundValues.get(n);
      if (c != null) {
        return getInstance(c);
      }
    } else {
      throw new IllegalStateException("Expected ClassNode, got: "
          + n.toString() + " (" + n.getClass() + ")");
    }
    // OK, n is a ClassNode, and has not been overridden with a call
    // to setDefaultImpl. Let's try to construct it!
    List<ConstructorDef> defs = new ArrayList<ConstructorDef>();
    for (ConstructorDef def : ((ClassNode) n).injectableConstructors) {
      boolean canInject = true;
      for (ConstructorArg arg : def.args) {
        String name = arg.getFullyQualifiedName(clazz);
      if (namespace.getNode(name) == null) { // XXX this is inadequate.  need to check to see if we can instantiate it too!
          canInject = false;
        }
      }
      if (canInject) {
        defs.add(def);
      }
    }

    // Now, find most specific def, or throw exception for non-comparable defs.
    if (defs.size() != 1) {
      throw new UnsupportedOperationException("Found " + defs.size()
          + " injectable constructors for class " + clazz + " (must be 1 for now).");
    }
    List<Object> args = new ArrayList<Object>();
    for(ConstructorArg arg : defs.get(0).args) {
      Node argNode = namespace.getNode(arg.getFullyQualifiedName(clazz));
      if(argNode instanceof ClassNode) {
        args.add(getInstance(((ClassNode)argNode).clazz));
      } else if(argNode instanceof NamedParameterNode){
        args.add(boundValues.get(argNode));
      }
    }
    try {
      return defs.get(0).constructor.newInstance(args.toArray());
    } catch(IllegalArgumentException e) {
      throw new IllegalStateException("Could not invoke constructor " + defs.get(0).constructor + " with args " + Arrays.toString(args.toArray()) + ": " + e.getMessage(), e);
    }
  }
}
