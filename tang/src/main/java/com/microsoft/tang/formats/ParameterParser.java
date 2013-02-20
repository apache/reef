package com.microsoft.tang.formats;

import java.lang.reflect.Constructor;

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.ReflectionUtilities;

public class ParameterParser {
  MonotonicMap<String, Constructor<? extends ExternalConstructor<?>>> parsers = new MonotonicMap<>();

  public void addParser(Class<? extends ExternalConstructor<?>> ec) throws BindException {
    Constructor<? extends ExternalConstructor<?>> c;
    try {
      c = ec.getConstructor(String.class);
    } catch (NoSuchMethodException e) {
      throw new BindException("Constructor "
          + ReflectionUtilities.getFullName(ec) + "(String) does not exist!", e);
    }
    c.setAccessible(true);
    Class<?> tc = ReflectionUtilities.getInterfaceTarget(
        ExternalConstructor.class, ec);
    parsers.put(ReflectionUtilities.getFullName(tc), c);
  }

  public void mergeIn(ParameterParser p) {
    for (String s : p.parsers.keySet()) {
      if (!parsers.containsKey(s)) {
        parsers.put(s, p.parsers.get(s));
      } else {
        if (!parsers.get(s).equals(p.parsers.get(s))) {
          throw new IllegalArgumentException(
              "Conflict detected when merging parameter parsers! To parse " + s
                  + " I have a: " + parsers.get(s)
                  + " the other instance has a: " + p.parsers.get(s));
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T parse(Class<T> c, String s) {
    String name = ReflectionUtilities.getFullName(c);
    if (parsers.containsKey(name)) {
      try {
        return (T) (parsers.get(name).newInstance(s).newInstance());
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException("Error invoking constructor for "
            + name, e);
      }
    } else {
      Class<?> d = ReflectionUtilities.boxClass(c);
      if (d == String.class) {
        return (T) s;
      }
      if (d == Byte.class) {
        return (T) (Byte) Byte.parseByte(s);
      }
      if (d == Character.class) {
        return (T) (Character) s.charAt(0);
      }
      if (d == Short.class) {
        return (T) (Short) Short.parseShort(s);
      }
      if (d == Integer.class) {
        return (T) (Integer) Integer.parseInt(s);
      }
      if (d == Long.class) {
        return (T) (Long) Long.parseLong(s);
      }
      if (d == Float.class) {
        return (T) (Float) Float.parseFloat(s);
      }
      if (d == Double.class) {
        return (T) (Double) Double.parseDouble(s);
      }
      if (d == Void.class) {
        throw new ClassCastException("Can't instantiate void");
      }
      throw new UnsupportedOperationException("Don't know how to parse a " + c);
    }
  }

}
