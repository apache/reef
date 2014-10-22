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
package com.microsoft.tang.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;

public class ReflectionUtilities {
  /**
   *  This is used to split Java classnames.  Currently, we split on . and on $
   */
  public final static String regexp = "[\\.\\$]";
  /**
   * A map from numeric classes to the number of bits used by their representations.
   */
  private final static Map<Class<?>, Integer> sizeof = new HashMap<>();
  static {
    sizeof.put(Byte.class, Byte.SIZE);
    sizeof.put(Short.class, Short.SIZE);
    sizeof.put(Integer.class, Integer.SIZE);
    sizeof.put(Long.class, Long.SIZE);
    sizeof.put(Float.class, Float.SIZE);
    sizeof.put(Double.class, Double.SIZE);
  }

  /**
   * Given a primitive type, return its boxed representation.
   * 
   * Examples:
   * boxClass(int.class) -> Integer.class
   * boxClass(String.class) -> String.class
   * 
   * @param c The class to be boxed.
   * @return The boxed version of c, or c if it is not a primitive type.
   */
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
  
  /**
   * Given a Type, return all of the classes it extends and interfaces it implements (including the class itself).
   * 
   * Examples:
   * Integer.class -> {Integer.class, Number.class, Object.class}
   * T -> Object
   * ? -> Object
   * HashSet<T> -> {HashSet<T>, Set<T>, Collection<T>, Object}
   * FooEventHandler -> {FooEventHandler, EventHandler<Foo>, Object}
   * 
   */
  public static Iterable<Type> classAndAncestors(Type c) {
    List<Type> workQueue = new ArrayList<>();

    workQueue.add(c);
    if(getRawClass(c).isInterface()){
      workQueue.add(Object.class);
    }
    for(int i = 0; i < workQueue.size(); i++) {
      c = workQueue.get(i);
      
      if(c instanceof Class) {
        Class<?> clz = (Class<?>)c;
        final Type sc = clz.getSuperclass();
        if(sc != null) workQueue.add(sc); //c.getSuperclass());
        workQueue.addAll(Arrays.asList(clz.getGenericInterfaces()));
      } else if(c instanceof ParameterizedType) {
        ParameterizedType pt = (ParameterizedType)c;
        Class<?> rawPt = (Class<?>)pt.getRawType();
        final Type sc = rawPt.getSuperclass();
//        workQueue.add(pt);
//        workQueue.add(rawPt);
        if(sc != null) workQueue.add(sc);
        workQueue.addAll(Arrays.asList(rawPt.getGenericInterfaces()));
      } else if (c instanceof WildcardType) {
        workQueue.add(Object.class); // XXX not really correct, but close enough?
      } else if (c instanceof TypeVariable) {
        workQueue.add(Object.class); // XXX not really correct, but close enough?
      } else {
        throw new RuntimeException(c.getClass() + " " + c + " is of unknown type!");
      }
    }
    return workQueue;
  }

  /**
   * Check to see if one class can be coerced into another.  A class is
   * coercable to another if, once both are boxed, the target class is a 
   * superclass or implemented interface of the source class.
   * 
   * If both classes are numeric types, then this method returns true iff
   * the conversion will not result in a loss of precision.
   * 
   * TODO: Float and double are currently coercible to int and long.  This is a bug.
   */
  public static boolean isCoercable(Class<?> to, Class<?> from) {
    to = boxClass(to);
    from = boxClass(from);
    if (Number.class.isAssignableFrom(to)
        && Number.class.isAssignableFrom(from)) {
      return sizeof.get(from) <= sizeof.get(to);
    }
    return to.isAssignableFrom(from);
  }
  /**
   * Lookup the provided name using the provided classloader. This method
   * includes special handling for primitive types, which can be looked up
   * by short name (all other types need to be looked up by long name).
   * 
   * @throws ClassNotFoundException
   */
  public static Class<?> classForName(String name, ClassLoader loader)
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
  public static String getSimpleName(Type name) {
    final Class<?> clazz = getRawClass(name);
    final String[] nameArray = clazz.getName().split(regexp);
    final String ret = nameArray[nameArray.length - 1];
    if(ret.length() == 0) {
      throw new IllegalArgumentException("Class " + name + " has zero-length simple name.  Can't happen?!?");
    }
    return ret;
  }
  /**
   * Return the full name of the raw type of the provided Type.
   * 
   * Examples:
   * 
   * java.lang.String.class -> "java.lang.String"
   * Set<String> -> "java.util.Set"  // such types can occur as constructor arguments, for example
   * 
   * @param name
   * @return
   */
  public static String getFullName(Type name) {
    return getRawClass(name).getName();
  }
  /**
   * Return the full name of the provided field.  This will be globally
   * unique.  Following Java semantics, the full name will have all the
   * generic parameters stripped out of it.
   * 
   * Example:
   * 
   * Set<X> { int size; } -> java.util.Set.size
   */
  public static String getFullName(Field f) {
    return getFullName(f.getDeclaringClass()) + "." + f.getName();
  }
  /**
   * This method takes a class called clazz that *directly* implements a generic interface or generic class, iface.
   * Iface should take a single parameter, which this method will return.
   * 
   * TODO This is only tested for interfaces, and the type parameters associated with method arguments.
   * TODO Not sure what we should do in the face of deeply nested generics (eg: Set<Set<String>)
   * TODO Recurse up the class hierarchy in case there are intermediate interfaces
   * 
   * @param iface A generic interface; we're looking up it's first (and only) parameter.
   * @param type A type that is more specific than clazz, or clazz if no such type is available.
   * @return The class implemented by the interface, or null(?) if the instantiation was not generic.
   * @throws IllegalArgumentException if clazz does not directly implement iface.
   */
  static public Type getInterfaceTarget(final Class<?> iface, final Type type) throws IllegalArgumentException {
    if(type instanceof ParameterizedType) {
      final ParameterizedType pt = (ParameterizedType)type;
      if(iface.isAssignableFrom((Class<?>)pt.getRawType())) {
        Type t = pt.getActualTypeArguments()[0];
        return t;
      } else {
        throw new IllegalArgumentException("Parameterized type " + type + " does not extend " + iface);
      }
    } else if(type instanceof Class) {
      final Class<?> clazz = (Class<?>)type;

      if(!clazz.equals(type)) {
        throw new IllegalArgumentException();
      }
      
      ArrayList<Type> al = new ArrayList<>();
      al.addAll(Arrays.asList(clazz.getGenericInterfaces()));
      Type sc = clazz.getGenericSuperclass();
      if(sc != null) al.add(sc);
      
      final Type[] interfaces = al.toArray(new Type[0]);
      
      for (Type genericNameType : interfaces) {
        if (genericNameType instanceof ParameterizedType) {
          ParameterizedType ptype = (ParameterizedType) genericNameType;
          if (ptype.getRawType().equals(iface)) {
            Type t = ptype.getActualTypeArguments()[0];
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
   * @return T if clazz implements Name<T>, null otherwise
   * @throws BindException
   *           If clazz's definition incorrectly uses Name or @NamedParameter
   */
  static public Type getNamedParameterTargetOrNull(Class<?> clazz)
      throws ClassHierarchyException {
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
  
    boolean hasMultipleInterfaces = (allInterfaces.length > 1);
    boolean implementsName;
    Type parameterClass = null;
    try {
      parameterClass = getInterfaceTarget(Name.class, clazz);
      implementsName = true;
    } catch(IllegalArgumentException e) {
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
   * 
   * Examples:
   * java.util.Set<String> -> java.util.Set
   * ? extends T -> Object
   * T -> Object
   * ? -> Object
   */
  public static Class<?> getRawClass(Type clazz) {
    if(clazz instanceof Class) {
      return (Class<?>)clazz;
    } else if (clazz instanceof ParameterizedType) {
      return (Class<?>)((ParameterizedType)clazz).getRawType();
    } else if (clazz instanceof WildcardType) {
      return Object.class; // XXX not really correct, but close enough?
    } else if (clazz instanceof TypeVariable) {
      return Object.class; // XXX not really correct, but close enough?
    } else {
      System.err.println("Can't getRawClass for " + clazz + " of unknown type " + clazz.getClass());
      throw new IllegalArgumentException("Can't getRawClass for " + clazz + " of unknown type " + clazz.getClass());
    }
  }
}
