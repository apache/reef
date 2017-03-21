/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.profiler;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.reef.tang.Aspect;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.types.ConstructorDef;
import org.apache.reef.tang.util.MonotonicHashMap;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * A graphical profiler class that instruments Tang-based Wake applications.
 */
public class WakeProfiler implements Aspect {
  private static final Logger LOG = Logger.getLogger(WakeProfiler.class.toString());
  private final Map<Object, Vertex<?>> vertexObject = new MonotonicHashMap<>();
  private final Map<InjectionFuture<?>, Object> futures = new MonotonicHashMap<>();
  private final Map<Object, Stats> stats = new MonotonicHashMap<>();

  @Override
  public Aspect createChildAspect() {
    return this;
  }

  @SuppressWarnings("unchecked")
  private <T> Vertex<T> getVertex(final T t) {
    if (t instanceof Set) {
      return (Vertex<T>) newSetVertex((Set<?>) t);
    } else {
      Vertex<T> v = (Vertex<T>) vertexObject.get(t);
      // Add dummy vertices for objects that were bound volatile.
      if (v == null) {
        v = new Vertex<>(t);
        vertexObject.put(t, v);
      }
      return v;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> Vertex<T> getFuture(final InjectionFuture<T> future) {
    return getVertex((T) futures.get(future));
  }

  @SuppressWarnings("unchecked")
  private <T> Vertex<?> newSetVertex(final Set<T> s) {
    if (vertexObject.containsKey(s)) {
      return vertexObject.get(s);
    }
    if (s.size() > 1) {
      LOG.fine("new set of size " + s.size());
      final Vertex<?>[] sArgs = new Vertex[s.size()];
      int k = 0;
      for (final Object p : s) {
        sArgs[k] = getVertex(p);
        k++;
      }
      final Vertex<Set<T>> sv = new Vertex<>(s, null, sArgs);
      vertexObject.put(s, sv);
      return sv;
    } else {
      final Object p = s.iterator().next();
      final Vertex<?> w = getVertex(p);
      // alias the singleton set to its member
      vertexObject.put(s, w);
      return w;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T inject(final ConstructorDef<T> constructorDef, final Constructor<T> constructor, final Object[] args)
      throws InvocationTargetException, IllegalAccessException, IllegalArgumentException, InstantiationException {
    final Vertex<?>[] vArgs = new Vertex[args.length];
    for (int i = 0; i < args.length; i++) {
      final Object o = args[i];
      final Vertex<?> v = getVertex(o);
      if (o instanceof Set) {
        LOG.fine("Got a set arg for " + constructorDef + " length " + ((Set<?>) o).size());
      }
      vArgs[i] = v;
    }

    T ret;
    final Class<T> clazz = constructor.getDeclaringClass();
    boolean isEventHandler = false;
    for (final Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals("onNext")) { // XXX hack: Interpose on "event handler in spirit"
        isEventHandler = true;
      }
    }
    if (isEventHandler) {
      try {
        if (Modifier.isFinal(clazz.getDeclaredMethod("onNext", Object.class).getModifiers())) {
          throw new Exception(ReflectionUtilities.getFullName(clazz) + ".onNext() is final; cannot intercept it");
        }
        final Stats s = new Stats();
        final Enhancer e = new Enhancer();
        e.setSuperclass(clazz);
        e.setCallback(new MethodInterceptor() {

          @Override
          public Object intercept(final Object object, final Method method, final Object[] args,
                                  final MethodProxy methodProxy) throws Throwable {

            if (method.getName().equals("onNext")) {
              final long start = System.nanoTime();
              final Object o = methodProxy.invokeSuper(object, args);
              final long stop = System.nanoTime();

              s.getMessageCount().incrementAndGet();
              s.getSumLatency().addAndGet(stop - start);

              return o;

            } else {
              return methodProxy.invokeSuper(object, args);
            }
          }
        });
        ret = (T) e.create(constructor.getParameterTypes(), args);
        stats.put(ret, s);
      } catch (final Exception e) {
        LOG.warning("Wake profiler could not intercept event handler: " + e.getMessage());
        ret = constructor.newInstance(args);
      }
    } else {
      ret = constructor.newInstance(args);
    }
    final Vertex<T> v = new Vertex<>(ret, constructorDef, vArgs);
    vertexObject.put(ret, v);
    return ret;
  }

  @Override
  public <T> void injectionFutureInstantiated(final InjectionFuture<T> arg0, final T arg1) {
    if (!futures.containsKey(arg0)) {
      LOG.warning("adding future " + arg0 + " instance " + arg1);
      futures.put(arg0, arg1);
      getVertex(arg1);
    }
  }

  private String jsonEscape(final String s) {
    return s
        .replaceAll("\\\\", "\\\\\\\\")
        .replaceAll("\\\"", "\\\\\"")
        .replaceAll("/", "\\\\/")
        .replaceAll("\b", "\\\\b")
        .replaceAll("\f", "\\\\f")
        .replaceAll("\n", "\\\\n")
        .replaceAll("\r", "\\\\r")
        .replaceAll("\t", "\\\\t");

  }

  private String join(final String sep, final List<String> tok) {
    if (tok.size() == 0) {
      return "";
    }
    final StringBuffer sb = new StringBuffer(tok.get(0));
    for (int i = 1; i < tok.size(); i++) {
      sb.append(sep + tok.get(i));
    }
    return sb.toString();
  }

  private boolean whitelist(final Object o) {
    return true;
  }

  public String objectGraphToString() {
    final List<Vertex<?>> vertices = new ArrayList<>();
    final Map<Vertex<?>, Integer> offVertex = new MonotonicHashMap<>();

    final StringBuffer sb = new StringBuffer("{\"nodes\":[\n");

    final List<String> nodes = new ArrayList<>();
    final LinkedList<Vertex<?>> workQueue = new LinkedList<>();
    for (final Object o : vertexObject.keySet()) {
      if (whitelist(o)) {
        workQueue.add(getVertex(o));
      }
    }
    for (final Object o : futures.values()) {
      if (!vertexObject.containsKey(o) && whitelist(o)) {
        workQueue.add(getVertex(o));
      }
    }
    while (!workQueue.isEmpty()) {
      final Vertex<?> v = workQueue.removeFirst();
      LOG.warning("Work queue " + v);

      final Object o = v.getObject();
      final String s;
      final String tooltip;
      if (o instanceof InjectionFuture) {
        s = null;
        tooltip = null;
      } else if (o instanceof String) {
        s = "\"" + ((String) o) + "\"";
        tooltip = null;
      } else if (o instanceof Number) {
        s = o.toString();
        tooltip = null;
      } else if (o instanceof Set) {
        LOG.warning("Set of size " + ((Set<?>) o).size() + " with " + v.getOutEdges().length + " out edges");
        s = "{...}";
        tooltip = null;
      } else {
        final Stats stat = stats.get(o);
        if (stat != null) {
          final long cnt = stat.getMessageCount().get();
          final long lat = stat.getSumLatency().get();
          tooltip = ",\"count\":" + cnt + ",\"latency\":\"" + (((double) lat) / (((double) cnt) * 1000000.0) + "\"");
          // quote the latency, since it might be nan
        } else {
          tooltip = null;
        }
        s = removeEnhancements(o.getClass().getSimpleName());
      }
      if (s != null) {
        offVertex.put(v, vertices.size());
        vertices.add(v);
        if (tooltip == null) {
          nodes.add("{\"name\":\"" + jsonEscape(s) + "\"}");
        } else {
          nodes.add("{\"name\":\"" + jsonEscape(s) + "\"" + tooltip + "}");
        }

      }
    }
    sb.append(join(",\n", nodes));
    sb.append("],\n\"links\":[");
    final List<String> links = new ArrayList<>();
    for (final Vertex<?> v : vertices) {
      for (final Vertex<?> w : v.getOutEdges()) {
        LOG.fine("pointing object" + v.getObject());
        LOG.fine("pointed to object " + w.getObject());
        if (w.getObject() instanceof InjectionFuture) {
          final Vertex<?> futureTarget = getFuture((InjectionFuture<?>) w.getObject()); //futures.get(w.getObject());
          final Integer off = offVertex.get(futureTarget);
          LOG.fine("future target " + futureTarget + " off = " + off);
          if (off != null) {
            links.add("{\"target\":" + offVertex.get(v) + ",\"source\":" + off + ",\"value\":" + 1.0 +
                ",\"back\":true}");
          }
        } else {
          final Integer off = offVertex.get(w);
          if (off != null) {
            final Stats s = stats.get(w.getObject());
            if (s != null) {
              links.add("{\"source\":" + offVertex.get(v) + ",\"target\":" + off + ",\"value\":" +
                  (s.getMessageCount().get() + 3.0) + "}");
            } else {
              links.add("{\"source\":" + offVertex.get(v) + ",\"target\":" + off + ",\"value\":" + 1.0 + "}");
            }
          }
        }
      }
    }
    sb.append(join(",\n", links));
    sb.append("]}");
    LOG.info("JSON: " + sb.toString());
    return sb.toString();
  }

  private String removeEnhancements(final String simpleName) {
    return simpleName.replaceAll("\\$\\$.+$", "");
  }

  private final class Stats {
    private AtomicLong messageCount = new AtomicLong(0);
    private AtomicLong sumLatency = new AtomicLong(0);

    AtomicLong getMessageCount() {
      return messageCount;
    }

    AtomicLong getSumLatency() {
      return sumLatency;
    }
  }

}
