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
package org.apache.reef.tang.util;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.ClassHierarchyException;

import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

public final class ReflectionUtilities {
  /**
   * This is used to split Java classnames.  Currently, we split on . and on $
   */
  public static final String REGEXP = "[\\.\\$]";

  /**
   * A map from numeric classes to the number of bits used by their representations.
   */
  private static final Map<Class<?>, Integer> SIZEOF = new HashMap<>();

  static {
    SIZEOF.put(Byte.class, Byte.SIZE);
    SIZEOF.put(Short.class, Short.SIZE);
    SIZEOF.put(Integer.class, Integer.SIZE);
    SIZEOF.put(Long.class, Long.SIZE);
    SIZEOF.put(Float.class, Float.SIZE);
    SIZEOF.put(Double.class, Double.SIZE);
  }

  /**
   * Given a primitive type, return its boxed representation.
   * <p>
   * Examples:
   * <pre>{@code
   * boxClass(int.class) -> Integer.class
   * boxClass(String.class) -> String.class
   * }</pre>
   * @param c The class to be boxed.
   * @return The boxed version of c, or c if it is not a primitive type.
   */
  public static Class<?> boxClass(final Class<?> c) {
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

  /**
   * Given a Type, return all of the classes it extends and interfaces it implements (including the class itself).
   * <p>
   * Examples:
   * <pre>{@code
   * Integer.class -> {Integer.class, Number.class, Object.class}
   * T -> Object
   * ? -> Object
   * HashSet<T> -> {HashSet<T>, Set<T>, Collection<T>, Object}
   * FooEventHandler -> {FooEventHandler, EventHandler<Foo>, Object}
   * }</pre>
   */
  public static Iterable<Type> classAndAncestors(final Type c) {
    final List<Type> workQueue = new ArrayList<>();

    Type clazz = c;
    workQueue.add(clazz);
    if (getRawClass(clazz).isInterface()) {
      workQueue.add(Object.class);
    }
    for (int i = 0; i < workQueue.size(); i++) {
      clazz = workQueue.get(i);

      if (clazz instanceof Class) {
        final Class<?> clz = (Class<?>) clazz;
        final Type sc = clz.getSuperclass();
        if (sc != null) {
          workQueue.add(sc); //c.getSuperclass());
        }
        workQueue.addAll(Arrays.asList(clz.getGenericInterfaces()));
      } else if (clazz instanceof ParameterizedType) {
        final ParameterizedType pt = (ParameterizedType) clazz;
        final Class<?> rawPt = (Class<?>) pt.getRawType();
        final Type sc = rawPt.getSuperclass();
//        workQueue.add(pt);
//        workQueue.add(rawPt);
        if (sc != null) {
          workQueue.add(sc);
        }
        workQueue.addAll(Arrays.asList(rawPt.getGenericInterfaces()));
      } else if (clazz instanceof WildcardType) {
        workQueue.add(Object.class); // XXX not really correct, but close enough?
      } else if (clazz instanceof TypeVariable) {
        workQueue.add(Object.class); // XXX not really correct, but close enough?
      } else {
        throw new RuntimeException(clazz.getClass() + " " + clazz + " is of unknown type!");
      }
    }
    return workQueue;
  }

  /**
   * Check to see if one class can be coerced into another.  A class is
   * coercable to another if, once both are boxed, the target class is a
   * superclass or implemented interface of the source class.
   * <p>
   * If both classes are numeric types, then this method returns true iff
   * the conversion will not result in a loss of precision.
   * <p>
   * TODO: Float and double are currently coercible to int and long.  This is a bug.
   */
  public static boolean isCoercable(final Class<?> to, final Class<?> from) {
    final Class<?> boxedTo = boxClass(to);
    final Class<?> boxedFrom = boxClass(from);
    if (Number.class.isAssignableFrom(boxedTo)
        && Number.class.isAssignableFrom(boxedFrom)) {
      return SIZEOF.get(boxedFrom) <= SIZEOF.get(boxedTo);
    }
    return boxedTo.isAssignableFrom(boxedFrom);
  }

  /**
   * Lookup the provided name using the provided classloader. This method
   * includes special handling for primitive types, which can be looked up
   * by short name (all other types need to be looked up by long name).
   *
   * @throws ClassNotFoundException
   */
  public static Class<?> classForName(final String name, final ClassLoader loader)
      throws ClassNotFoundException {
    if (name.startsWith("[")) {
      throw new UnsupportedOperationException("No support for arrays, etc.  Name was: " + name);
    } else if (name.equals("boolean")) {
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

  /**
   * Get the simple name of the class. This varies from the one in Class, in
   * that it returns "1" for Classes like java.lang.String$1 In contrast,
   * String.class.getSimpleName() returns "", which is not unique if
   * java.lang.String$2 exists, causing all sorts of strange bugs.
   *
   * @param name
   * @return
   */
  public static String getSimpleName(final Type name) {
    final Class<?> clazz = getRawClass(name);
    final String[] nameArray = clazz.getName().split(REGEXP);
    final String ret = nameArray[nameArray.length - 1];
    if (ret.length() == 0) {
      throw new IllegalArgumentException("Class " + name + " has zero-length simple name.  Can't happen?!?");
    }
    return ret;
  }

  /**
   * Return the full name of the raw type of the provided Type.
   * <p>
   * Examples:
   * <pre>{@code
   * java.lang.String.class -> "java.lang.String"
   * Set<String> -> "java.util.Set"  // such types can occur as constructor arguments, for example
   * }</pre>
   * @param name
   * @return
   */
  public static String getFullName(final Type name) {
    return getRawClass(name).getName();
  }

  /**
   * Return the full name of the provided field.  This will be globally
   * unique.  Following Java semantics, the full name will have all the
   * generic parameters stripped out of it.
   * <p>
   * Example:
   * <pre>{@code
   * Set<X> { int size; } -> java.util.Set.size
   * }</pre>
   */
  public static String getFullName(final Field f) {
    return getFullName(f.getDeclaringClass()) + "." + f.getName();
  }

  /**
   * This method takes a class called clazz that *directly* implements a generic interface or generic class, iface.
   * Iface should take a single parameter, which this method will return.
   * <p>
   * TODO This is only tested for interfaces, and the type parameters associated with method arguments.
   * TODO Not sure what we should do in the face of deeply nested generics (eg: {@code Set<Set<String>})
   * TODO Recurse up the class hierarchy in case there are intermediate interfaces
   *
   * @param iface A generic interface; we're looking up it's first (and only) parameter.
   * @param type  A type that is more specific than clazz, or clazz if no such type is available.
   * @return The class implemented by the interface, or null(?) if the instantiation was not generic.
   * @throws IllegalArgumentException if clazz does not directly implement iface.
   */
  public static Type getInterfaceTarget(final Class<?> iface, final Type type) throws IllegalArgumentException {
    if (type instanceof ParameterizedType) {
      final ParameterizedType pt = (ParameterizedType) type;
      if (iface.isAssignableFrom((Class<?>) pt.getRawType())) {
        final Type t = pt.getActualTypeArguments()[0];
        return t;
      } else {
        throw new IllegalArgumentException("Parameterized type " + type + " does not extend " + iface);
      }
    } else if (type instanceof Class) {
      final Class<?> clazz = (Class<?>) type;

      if (!clazz.equals(type)) {
        throw new IllegalArgumentException("The clazz is "  + clazz + " and the type is "
                + type + ". They must be equal to each other.");
      }

      final ArrayList<Type> al = new ArrayList<>();
      al.addAll(Arrays.asList(clazz.getGenericInterfaces()));
      final Type sc = clazz.getGenericSuperclass();
      if (sc != null) {
        al.add(sc);
      }

      final Type[] interfaces = al.toArray(new Type[0]);

      for (final Type genericNameType : interfaces) {
        if (genericNameType instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType) genericNameType;
          if (ptype.getRawType().equals(iface)) {
            final Type t = ptype.getActualTypeArguments()[0];
            return t;
          }
        }
      }
      throw new IllegalArgumentException(clazz + " does not directly implement " + iface);
    } else {
      throw new UnsupportedOperationException("Do not know how to get interface target of " + type);
    }
  }

  /**
   * @param clazz
   * @return T if clazz implements {@code Name<T>}, null otherwise
   * @throws org.apache.reef.tang.exceptions.BindException
   * If clazz's definition incorrectly uses Name or @NamedParameter
   */
  public static Type getNamedParameterTargetOrNull(final Class<?> clazz)
      throws ClassHierarchyException {
    final Annotation npAnnotation = clazz.getAnnotation(NamedParameter.class);
    final boolean hasSuperClass = clazz.getSuperclass() != Object.class;

    boolean isInjectable = false;
    boolean hasConstructor = false;
    // TODO Figure out how to properly differentiate between default and
    // non-default zero-arg constructors?
    final Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    if (constructors.length > 1) {
      hasConstructor = true;
    }
    if (constructors.length == 1) {
      final Constructor<?> c = constructors[0];
      final Class<?>[] p = c.getParameterTypes();
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
    for (final Constructor<?> c : constructors) {
      for (final Annotation a : c.getDeclaredAnnotations()) {
        if (a instanceof Inject) {
          isInjectable = true;
        }
      }
    }

    final Class<?>[] allInterfaces = clazz.getInterfaces();

    final boolean hasMultipleInterfaces = allInterfaces.length > 1;
    boolean implementsName;
    Type parameterClass = null;
    try {
      parameterClass = getInterfaceTarget(Name.class, clazz);
      implementsName = true;
    } catch (final IllegalArgumentException e) {
      implementsName = false;
    }

    if (npAnnotation == null) {
      if (implementsName) {
        throw new ClassHierarchyException("Named parameter " + getFullName(clazz)
            + " is missing its @NamedParameter annotation.");
      } else {
        return null;
      }
    } else {
      if (!implementsName) {
        throw new ClassHierarchyException("Found illegal @NamedParameter " + getFullName(clazz)
            + " does not implement Name<?>");
      }
      if (hasSuperClass) {
        throw new ClassHierarchyException("Named parameter " + getFullName(clazz)
            + " has a superclass other than Object.");
      }
      if (hasConstructor || isInjectable) {
        throw new ClassHierarchyException("Named parameter " + getFullName(clazz) + " has "
            + (isInjectable ? "an injectable" : "a") + " constructor. "
            + " Named parameters must not declare any constructors.");
      }
      if (hasMultipleInterfaces) {
        throw new ClassHierarchyException("Named parameter " + getFullName(clazz) + " implements "
            + "multiple interfaces.  It is only allowed to implement Name<T>");
      }
      if (parameterClass == null) {
        throw new ClassHierarchyException(
            "Missing type parameter in named parameter declaration.  " + getFullName(clazz)
                + " implements raw type Name, but must implement"
                + " generic type Name<T>.");
      }
      return parameterClass;
    }
  }

  /**
   * Coerce a Type into a Class.  This strips out any generic paramters, and
   * resolves wildcards and free parameters to Object.
   * <p>
   * Examples:
   * <pre>{@code
   * java.util.Set<String> -> java.util.Set
   * ? extends T -> Object
   * T -> Object
   * ? -> Object
   * }</pre>
   */
  public static Class<?> getRawClass(final Type clazz) {
    if (clazz instanceof Class) {
      return (Class<?>) clazz;
    } else if (clazz instanceof ParameterizedType) {
      return (Class<?>) ((ParameterizedType) clazz).getRawType();
    } else if (clazz instanceof WildcardType) {
      return Object.class; // XXX not really correct, but close enough?
    } else if (clazz instanceof TypeVariable) {
      return Object.class; // XXX not really correct, but close enough?
    } else {
      System.err.println("Can't getRawClass for " + clazz + " of unknown type " + clazz.getClass());
      throw new IllegalArgumentException("Can't getRawClass for " + clazz + " of unknown type " + clazz.getClass());
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private ReflectionUtilities() {
  }
}
