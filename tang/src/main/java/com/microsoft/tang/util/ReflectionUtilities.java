package com.microsoft.tang.util;

import java.util.HashMap;
import java.util.Map;

public class ReflectionUtilities {
  private static Map<Class<?>, Integer> sizeof = new HashMap<Class<?>, Integer>();
  static {
    sizeof.put(Byte.class, 8);
    sizeof.put(Short.class, 16);
    sizeof.put(Integer.class, 32);
    sizeof.put(Long.class, 64);
    sizeof.put(Float.class, 32);
    sizeof.put(Double.class, 64);
  }
  public static Class<?> boxClass(Class<?> c) {
    if(c.isPrimitive() && c != Class.class) {
      if(c == boolean.class) {
      return Boolean.class;
      } else if(c == byte.class) {
        return Byte.class;
      } else if(c == char.class) {
        return Character.class;
      } else if(c == short.class){
        return Short.class;
      } else if(c == int.class) {
        return Integer.class;
      } else if(c == long.class) {
        return Long.class;
      } else if(c == float.class) {
        return Float.class;
      } else if(c == double.class) {
        return Double.class;
      } else if(c == void.class) {
        return Void.class;
      } else {
        throw new UnsupportedOperationException("Encountered unknown primitive type!");
      }
    } else {
      return c;
    }
  }
  public static boolean isCoercable(Class<?> to, Class<?> from) {
    to = boxClass(to);
    from = boxClass(from);
    if(Number.class.isAssignableFrom(to) && Number.class.isAssignableFrom(from)) {
      return sizeof.get(from) <= sizeof.get(to);
    }
    return to.isAssignableFrom(from);
  }
  public static Class<?> classForName(String name) throws ClassNotFoundException {
    if(name.equals("boolean")) {
      return boolean.class;
    } else if(name.equals("byte")) {
      return byte.class;
    } else if(name.equals("char")) {
      return char.class;
    } else if(name.equals("short")) {
      return short.class;
    } else if(name.equals("int")) {
      return int.class;
    } else if(name.equals("long")) {
      return long.class;
    } else if(name.equals("float")) {
      return float.class;
    } else if(name.equals("double")) {
      return double.class;
    } else if(name.equals("void")) {
      return void.class;
    } else {
      // TODO: Might need to used passed in ClassLoaders here! 
      return Class.forName(name);
    }
  }
  @SuppressWarnings("unchecked")
  public static <T> T parse(Class<T> c, String s) {
    Class<?> d = boxClass(c);
    if(d == String.class) { return (T)s; }
    if(d == Byte.class) { return (T)(Byte)Byte.parseByte(s); }
    if(d == Character.class) { return (T)(Character)s.charAt(0); }
    if(d == Short.class){ return (T)(Short)Short.parseShort(s); }
    if(d == Integer.class) { return (T)(Integer)Integer.parseInt(s); }
    if(d == Long.class){ return (T)(Long)Long.parseLong(s); }
    if(d == Float.class) { return (T)(Float)Float.parseFloat(s); }
    if(d == Double.class) { return (T)(Double)Double.parseDouble(s); }
    if(d == Void.class) { throw new ClassCastException("Can't instantiate void"); }
    throw new UnsupportedOperationException("Don't know how to parse a " + c);
  }
}
