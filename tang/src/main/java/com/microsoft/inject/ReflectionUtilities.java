package com.microsoft.inject;

class ReflectionUtilities {
  static Class<?> boxClass(Class<?> c) {
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
  static boolean isCoercable(Class<?> to, Class<?> from) {
    to = boxClass(to);
    from = boxClass(from);
    if(Number.class.isAssignableFrom(to) && Number.class.isAssignableFrom(from)) {
      return true; // XXX not quite true.  Might be loss of precision...
    }
    return to.isAssignableFrom(from);
  }
  static Class<?> classForName(String name) throws ClassNotFoundException {
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
      return Class.forName(name);
    }
  }
}
