package com.microsoft.tang.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;

public class ReflectionUtilities {
  public final static String regexp = "[\\.\\$]";
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
    if (c.isPrimitive() && c != Class.class) {
      if (c == boolean.class) {
        return Boolean.class;
      } else if (c == byte.class) {
        return Byte.class;
      } else if (c == char.class) {
        return Character.class;
      } else if (c == short.class) {
        return Short.class;
      } else if (c == int.class) {
        return Integer.class;
      } else if (c == long.class) {
        return Long.class;
      } else if (c == float.class) {
        return Float.class;
      } else if (c == double.class) {
        return Double.class;
      } else if (c == void.class) {
        return Void.class;
      } else {
        throw new UnsupportedOperationException(
            "Encountered unknown primitive type!");
      }
    } else {
      return c;
    }
  }

  public static boolean isCoercable(Class<?> to, Class<?> from) {
    to = boxClass(to);
    from = boxClass(from);
    if (Number.class.isAssignableFrom(to)
        && Number.class.isAssignableFrom(from)) {
      return sizeof.get(from) <= sizeof.get(to);
    }
    return to.isAssignableFrom(from);
  }
  public static Class<?> classForName(String name, ClassLoader loader)
      throws ClassNotFoundException {
    if (name.equals("boolean")) {
      return boolean.class;
    } else if (name.equals("byte")) {
      return byte.class;
    } else if (name.equals("char")) {
      return char.class;
    } else if (name.equals("short")) {
      return short.class;
    } else if (name.equals("int")) {
      return int.class;
    } else if (name.equals("long")) {
      return long.class;
    } else if (name.equals("float")) {
      return float.class;
    } else if (name.equals("double")) {
      return double.class;
    } else if (name.equals("void")) {
      return void.class;
    } else {
      return loader.loadClass(name);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T parse(Class<T> c, String s) {
    Class<?> d = boxClass(c);
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

  /**
   * Get the simple name of the class. This varies from the one in Class, in
   * that it returns "1" for Classes like java.lang.String$1 In contrast,
   * String.class.getSimpleName() returns "", which is not unique if
   * java.lang.String$2 exists, causing all sorts of strange bugs.
   * 
   * @param name
   * @return
   */
  public static String getSimpleName(Class<?> name) {
    final String[] nameArray = name.getName().split(regexp);
    final String ret = nameArray[nameArray.length - 1];
    if(ret.length() == 0) {
      throw new IllegalArgumentException("Class " + name + " has zero-length simple name.  Can't happen?!?");
    }
    return ret;
  }
  public static String getFullName(Class<?> name) {
    return name.getName();
  }

  /**
   * @param clazz
   * @return T if clazz implements Name<T>, null otherwise
   * @throws BindException
   *           If clazz's definition incorrectly uses Name or @NamedParameter
   */
  static public Class<?> getNamedParameterTargetOrNull(Class<?> clazz)
      throws BindException {
    Annotation npAnnotation = clazz.getAnnotation(NamedParameter.class);
    boolean hasSuperClass = (clazz.getSuperclass() != Object.class);
  
    boolean isInjectable = false;
    boolean hasConstructor = false;
    // TODO Figure out how to properly differentiate between default and
    // non-default zero-arg constructors?
    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    if (constructors.length > 1) {
      hasConstructor = true;
    }
    if (constructors.length == 1) {
      Constructor<?> c = constructors[0];
      Class<?>[] p = c.getParameterTypes();
      if (p.length > 1) {
        // Multiple args. Definitely not implicit.
        hasConstructor = true;
      } else if (p.length == 1) {
        // One arg. Could be an inner class, in which case the compiler
        // included an implicit one parameter constructor that takes the
        // enclosing type.
        if (p[0] != clazz.getEnclosingClass()) {
          hasConstructor = true;
        }
      }
    }
    for (Constructor<?> c : constructors) {
      for (Annotation a : c.getDeclaredAnnotations()) {
        if (a instanceof Inject) {
          isInjectable = true;
        }
      }
    }
  
    Class<?>[] allInterfaces = clazz.getInterfaces();
    Type[] interfaces = clazz.getGenericInterfaces();
  
    boolean hasMultipleInterfaces = (allInterfaces.length > 1);
    boolean implementsName = false;
    Class<?> parameterClass = null;
    for (Type genericNameType : interfaces) {
      if (genericNameType instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) genericNameType;
        if (ptype.getRawType() == Name.class) {
          implementsName = true;
          Type t = ptype.getActualTypeArguments()[0];
          // It could be that the parameter is, itself a generic type. Not
          // sure if we should support this, but we do for now.
          if (t instanceof ParameterizedType) {
            // Get the underlying raw type of the parameter.
            t = ((ParameterizedType) t).getRawType();
          }
          parameterClass = (Class<?>) t;
        }
      }
    }
    if (npAnnotation == null) {
      if (implementsName) {
        throw new BindException(clazz
            + " is missing its @NamedParameter annotation.");
      } else {
        return null;
      }
    } else {
      if (!implementsName) {
        throw new BindException("Found illegal @NamedParameter " + clazz
            + " does not implement name");
      }
      if (hasSuperClass) {
        throw new BindException("Named parameter " + clazz
            + " has a superclass other than Object.");
      }
      if (hasConstructor || isInjectable) {
        throw new BindException("Named parameter " + clazz + " has "
            + (isInjectable ? "an injectable" : "a") + " constructor. "
            + " Name parameters must not delcare any constructors.");
      }
      if (hasMultipleInterfaces) {
        throw new BindException("Named parameter " + clazz + " implements "
            + "multiple interfaces.  It is only allowed to implement Name<T>");
      }
      if (parameterClass == null) {
        throw new BindException(
            "Missing type parameter in named parameter declaration.  " + clazz
                + " implements raw type Name, but must implement"
                + " generic type Name<T>.");
      }
      return parameterClass;
    }
  }
}
