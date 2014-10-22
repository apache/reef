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
package com.microsoft.wake.profiler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import com.microsoft.tang.Aspect;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.util.MonotonicHashMap;
import com.microsoft.tang.util.ReflectionUtilities;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Stage;
import com.microsoft.wake.rx.Observable;
import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.RxStage;

public class WakeProfiler implements Aspect {
  private final static Logger LOG = Logger.getLogger(WakeProfiler.class.toString());

  private final class Stats {
    AtomicLong messageCount = new AtomicLong(0);
    AtomicLong sumLatency = new AtomicLong(0);
  }
  
  private final Map<Object, Vertex<?>> vertex_object = new MonotonicHashMap<>();
  private final Map<InjectionFuture<?>, Object> futures = new MonotonicHashMap<>();
  private final Map<Object, Stats> stats = new MonotonicHashMap<>();
  @Override
  public Aspect createChildAspect() {
    return this;
  }

  @SuppressWarnings("unchecked")
  private <T> Vertex<T> getVertex(T t) {
    if(t instanceof Set) {
      return (Vertex<T>)newSetVertex((Set<?>)t);
    } else {
      Vertex<T> v = (Vertex<T>)vertex_object.get(t);
      // Add dummy vertices for objects that were bound volatile.
      if(v == null) {
        v = new Vertex<>(t);
        vertex_object.put(t, v);
      }
      return v;
    }
  }
  @SuppressWarnings("unchecked")
  private <T> Vertex<T> getFuture(InjectionFuture<T> future) {
      return getVertex((T)futures.get(future));
  }
  @SuppressWarnings("unchecked")
  private <T> Vertex<?> newSetVertex(Set<T> s) {
    if(vertex_object.containsKey(s)) {
      return (Vertex<Set<T>>) vertex_object.get(s);
    }
    if(s.size() > -1) {
      LOG.fine("new set of size " + s.size());
      Vertex<?>[] s_args = new Vertex[s.size()];
      int k = 0;
      for(Object p : s) {
        s_args[k] = getVertex(p);
        k++;
      }
      Vertex<Set<T>> sv = new Vertex<>(s, null, s_args);
      vertex_object.put(s, sv);
      return sv;
//    } else if(s.size() == 1) {
    } else {
      Object p = s.iterator().next();
      Vertex<?> w = getVertex(p);
      // alias the singleton set to its member
      vertex_object.put(s, w);
      return w;
    }
//    } else {
//    // ignore the empty set.
//  } */
  }
  @SuppressWarnings("unchecked")
  @Override
  public <T> T inject(ConstructorDef<T> constructorDef, Constructor<T> constructor, Object[] args) throws InvocationTargetException, IllegalAccessException, IllegalArgumentException, InstantiationException {
//    LOG.info("inject" + constructor + "->" + args.length);
    Vertex<?>[] v_args = new Vertex[args.length];
    for(int i = 0; i < args.length; i++) {
      Object o = args[i];
      Vertex<?> v = getVertex(o);
      if(o instanceof Set) {
        LOG.fine("Got a set arg for " + constructorDef + " length " + ((Set<?>)o).size());
      } else if(o instanceof InjectionFuture) {
//        LOG.info("Got injection future arg. "->" + o);
      }
      v_args[i] = v;
    }
    
    T ret;
    final Class<T> clazz = constructor.getDeclaringClass();
    boolean isEventHandler = false;
    for(Method m : clazz.getDeclaredMethods()) {
      if(m.getName().equals("onNext")) { // XXX hack: Interpose on "event handler in spirit"
        isEventHandler = true;
      }
    }
    if(isEventHandler) {
      try {
        if(Modifier.isFinal(clazz.getDeclaredMethod("onNext", Object.class).getModifiers())) {
          throw new Exception(ReflectionUtilities.getFullName(clazz) + ".onNext() is final; cannot intercept it");
        }
        final Stats s = new Stats();
        Enhancer e = new Enhancer();
        e.setSuperclass(clazz);
        e.setCallback(new MethodInterceptor() {
          
          @Override
          public Object intercept(Object object, Method method, Object[] args,
              MethodProxy methodProxy) throws Throwable {

            if(method.getName().equals("onNext")) {
              long start = System.nanoTime();
//              LOG.info(object + "." + method.getName() + " called");
              Object o = methodProxy.invokeSuper(object, args);
              long stop = System.nanoTime();
              
              s.messageCount.incrementAndGet();
              s.sumLatency.addAndGet(stop-start);
              
              return o;
              
            } else {
              return methodProxy.invokeSuper(object, args);
            }
          }
        });
        ret =(T) e.create(constructor.getParameterTypes(), args);
        stats.put(ret, s);
      } catch(Exception e) {
        LOG.warning("Wake profiler could not intercept event handler: " + e.getMessage());
        ret = constructor.newInstance(args);
      }
    } else {
      ret = constructor.newInstance(args);
    }
    Vertex<T> v = new Vertex<T>(ret, constructorDef, v_args);
    vertex_object.put(ret, v);
    return ret;
  }

  @Override
  public <T> void injectionFutureInstantiated(InjectionFuture<T> arg0, T arg1) {
    if(!futures.containsKey(arg0)) {
      LOG.warning("adding future " + arg0 + " instance " + arg1);
      futures.put(arg0, arg1);
      getVertex(arg1);
    }
  }

  private String jsonEscape(String s) {
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
  private String join(String sep, List<String> tok) {
    if(tok.size() == 0) { return ""; }
    StringBuffer sb = new StringBuffer(tok.get(0));
    for(int i = 1; i < tok.size(); i++) {
      sb.append(sep+tok.get(i));
    }
    return sb.toString();
  }
  private boolean whitelist(Object o) {
    return (true
        || (o instanceof InjectionFuture)
        || (o instanceof Set)
        || (o instanceof EventHandler)
        || (o instanceof Stage)
        || (o instanceof RxStage)
        || (o instanceof Observer)
        || (o instanceof Observable))
//        && !(o instanceof Set)
        ;
  }
  public String objectGraphToString() {
    List<Vertex<?>> vertices = new ArrayList<>();
    Map<Vertex<?>, Integer> off_vertex = new MonotonicHashMap<>();

    StringBuffer sb = new StringBuffer("{\"nodes\":[\n");

    List<String> nodes = new ArrayList<String>();
    LinkedList<Vertex<?>> workQueue = new LinkedList<>();
    for(Object o : vertex_object.keySet()) {
      if(whitelist(o)) {
        workQueue.add(getVertex(o));
      }
    }
    for(Object o : futures.values()) {
      if((!vertex_object.containsKey(o)) && whitelist(o)) {
        workQueue.add(getVertex(o));
      }
    }
    while(!workQueue.isEmpty()) {
      Vertex<?> v = workQueue.removeFirst();
      LOG.warning("Work queue " + v);
      
      Object o = v.getObject();
      final String s;
      final String tooltip;
      if(o instanceof InjectionFuture){
        s = null;
        tooltip = null;
      } else if(o instanceof String) {
        s = "\""+((String)o)+"\"";
        tooltip = null;
      } else if(o instanceof Number) {
        s = o.toString();
        tooltip = null;
      } else if(o instanceof Set){
        LOG.warning("Set of size " + ((Set<?>)o).size() + " with " + v.getOutEdges().length + " out edges");
        s = "{...}";
        tooltip = null;
////      } else if(false && (o instanceof EventHandler || o instanceof Stage)) {
////        s = jsonEscape(v.getObject().toString()); 
      } else {
        Stats stat = stats.get(o);
        if(stat != null) {
          long cnt = stat.messageCount.get();
          long lat = stat.sumLatency.get();
          tooltip = ",\"count\":" + cnt + ",\"latency\":\"" + (((double)lat)/(((double)cnt)*1000000.0)+"\""); // quote the latency, since it might be nan
        } else {
          tooltip = null;
        }
        s = removeEnhancements(o.getClass().getSimpleName());
      }
      if(s != null) {
        off_vertex.put(v, vertices.size());
        vertices.add(v);
        if(tooltip == null) {
          nodes.add("{\"name\":\""+jsonEscape(s)+"\"}");
        } else {
          nodes.add("{\"name\":\""+jsonEscape(s)+"\""+tooltip+"}");
        }
          
      }
    }
    sb.append(join(",\n", nodes));
    sb.append("],\n\"links\":[");
    List<String> links = new ArrayList<>();
    for(Vertex<?> v : vertices) {
      for(Vertex<?> w : v.getOutEdges()) {
        LOG.fine("pointing object" + v.getObject());
        LOG.fine("pointed to object " + w.getObject());
        if(w.getObject() instanceof InjectionFuture) {
          Vertex<?> futureTarget = getFuture((InjectionFuture<?>)w.getObject()); //futures.get(w.getObject());
          Integer off = off_vertex.get(futureTarget);
          LOG.fine("future target " + futureTarget + " off = " + off);
          if(off!=null) {
            links.add("{\"target\":"+off_vertex.get(v)+",\"source\":"+off+",\"value\":"+1.0+",\"back\":true}");
          }
        } else {
          Integer off = off_vertex.get(w);
          if(off != null) {
            Stats s = stats.get(w.getObject());
            if(s != null) {
              links.add("{\"source\":"+off_vertex.get(v)+",\"target\":"+off+",\"value\":" + (s.messageCount.get() + 3.0) + "}");
            } else {
              links.add("{\"source\":"+off_vertex.get(v)+",\"target\":"+off+",\"value\":" + 1.0 + "}");
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

  private String removeEnhancements(String simpleName) {
    return simpleName.replaceAll("\\$\\$.+$", "");
  }
  
}
