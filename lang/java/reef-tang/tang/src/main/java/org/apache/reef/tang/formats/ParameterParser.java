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
package org.apache.reef.tang.formats;

import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.util.MonotonicTreeMap;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ParameterParser {
  private static final Set<String> BUILTIN_NAMES = new HashSet<String>() {
    private static final long serialVersionUID = 1L;

    {
      Collections.addAll(this,
          String.class.getName(),
          Byte.class.getName(),
          Character.class.getName(),
          Short.class.getName(),
          Integer.class.getName(),
          Long.class.getName(),
          Float.class.getName(),
          Double.class.getName(),
          Boolean.class.getName(),
          Void.class.getName());
    }
  };
  private MonotonicTreeMap<String, Constructor<? extends ExternalConstructor<?>>> parsers = new MonotonicTreeMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void addParser(final Class<? extends ExternalConstructor<?>> ec) throws BindException {
    final Class<?> tc = (Class<?>) ReflectionUtilities.getInterfaceTarget(
        ExternalConstructor.class, ec);
    addParser((Class) tc, (Class) ec);
  }

  public <T, U extends T> void addParser(final Class<U> clazz, final Class<? extends ExternalConstructor<T>> ec)
      throws BindException {
    final Constructor<? extends ExternalConstructor<T>> c;
    try {
      c = ec.getDeclaredConstructor(String.class);
      c.setAccessible(true);
    } catch (final NoSuchMethodException e) {
      throw new BindException("Constructor "
          + ReflectionUtilities.getFullName(ec) + "(String) does not exist!", e);
    }
    c.setAccessible(true);
    parsers.put(ReflectionUtilities.getFullName(clazz), c);
  }

  public void mergeIn(final ParameterParser p) {
    for (final String s : p.parsers.keySet()) {
      if (!parsers.containsKey(s)) {
        parsers.put(s, p.parsers.get(s));
      } else {
        if (!parsers.get(s).equals(p.parsers.get(s))) {
          throw new IllegalArgumentException(
              "Conflict detected when merging parameter parsers! To parse " + s
                  + " I have a: " + ReflectionUtilities.getFullName(parsers.get(s).getDeclaringClass())
                  + " the other instance has a: "
                  + ReflectionUtilities.getFullName(p.parsers.get(s).getDeclaringClass()));
        }
      }
    }
  }

  public <T> T parse(final Class<T> c, final String s) {
    if (c.isEnum()) {
      return parseEnum(c, s);
    }
    final Class<?> d = ReflectionUtilities.boxClass(c);
    for (final Type e : ReflectionUtilities.classAndAncestors(d)) {
      final String name = ReflectionUtilities.getFullName(e);
      if (parsers.containsKey(name)) {
        final T ret = parse(name, s);
        if (c.isAssignableFrom(ret.getClass())) {
          return ret;
        } else {
          throw new ClassCastException("Cannot cast from " + ret.getClass() + " to " + c);
        }
      }
    }
    return parse(ReflectionUtilities.getFullName(d), s);
  }

  @SuppressWarnings("unchecked")
  private <T> T parseEnum(final Class<T> c, final String s) {
    try {
      final Method valueOf = c.getMethod("valueOf", String.class);
      return (T) valueOf.invoke(null, s);
    } catch (final NoSuchMethodException e) {
      throw new RuntimeException("Static .valueOf(String) method not found at " + c, e);
    } catch (final InvocationTargetException e) {
      throw new RuntimeException("Cannot invoke .valueOf(String) method of " + c, e);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Cannot create instance of " + c + " - is it public?", e);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T parse(final String name, final String value) {
    if (parsers.containsKey(name)) {
      try {
        return (T) (parsers.get(name).newInstance(value).newInstance());
      } catch (final ReflectiveOperationException e) {
        throw new IllegalArgumentException("Error invoking constructor for "
            + name, e);
      }
    } else {
      if (name.equals(String.class.getName())) {
        return (T) value;
      }
      if (name.equals(Byte.class.getName())) {
        return (T) (Byte) Byte.parseByte(value);
      }
      if (name.equals(Character.class.getName())) {
        return (T) (Character) value.charAt(0);
      }
      if (name.equals(Short.class.getName())) {
        return (T) (Short) Short.parseShort(value);
      }
      if (name.equals(Integer.class.getName())) {
        return (T) (Integer) Integer.parseInt(value);
      }
      if (name.equals(Long.class.getName())) {
        return (T) (Long) Long.parseLong(value);
      }
      if (name.equals(Float.class.getName())) {
        return (T) (Float) Float.parseFloat(value);
      }
      if (name.equals(Double.class.getName())) {
        return (T) (Double) Double.parseDouble(value);
      }
      if (name.equals(Boolean.class.getName())) {
        return (T) (Boolean) Boolean.parseBoolean(value);
      }
      if (name.equals(Void.class.getName())) {
        throw new ClassCastException("Can't instantiate void");
      }
      throw new UnsupportedOperationException("Don't know how to parse a " + name);
    }
  }

  public boolean canParse(final String name) {
    return parsers.containsKey(name) || BUILTIN_NAMES.contains(name);
  }
}
