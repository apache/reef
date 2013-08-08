package com.microsoft.tang.util;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;

public class Tint {

  public Tint() {
    this(new URL[0]);
  }
  public Tint(URL[] jars) {
    this(jars, false);
  }
  public Tint(URL[] jars, boolean checkTang) {
    Object[] args = new Object[jars.length + 4];
    for(int i = 0; i < jars.length; i++){
      args[i] = jars[i];
    }
    args[args.length-1] = new TypeAnnotationsScanner();
    args[args.length-2] = new SubTypesScanner();
    args[args.length-3] = new MethodAnnotationsScanner();
    args[args.length-4] = new MethodParameterScanner();
    Reflections r = new Reflections(args);
    Set<Class<?>> classes = new HashSet<>();
    Set<String> strings = new TreeSet<>();

    // Workaround bug in Reflections by keeping things stringly typed, and using Tang to parse them.
//  Set<Constructor<?>> injectConstructors = (Set<Constructor<?>>)(Set)r.getMethodsAnnotatedWith(Inject.class);
//  for(Constructor<?> c : injectConstructors) {
//    classes.add(c.getDeclaringClass());
//  }
    Set<String> injectConstructors = r.getStore().getConstructorsAnnotatedWith(ReflectionUtilities.getFullName(Inject.class));
    for(String s : injectConstructors) {
      strings.add(s.replaceAll("\\.<.+$",""));
    }
    Set<String> parameterConstructors = r.getStore().get(MethodParameterScanner.class, ReflectionUtilities.getFullName(Parameter.class));
    for(String s : parameterConstructors) {
      strings.add(s.replaceAll("\\.<.+$",""));
    }
//    Set<Class> r.getConstructorsWithAnyParamAnnotated(Parameter.class);
//    for(Constructor<?> c : parameterConstructors) {
//      classes.add(c.getDeclaringClass());
//    }
    classes.addAll(r.getTypesAnnotatedWith(DefaultImplementation.class));
    classes.addAll(r.getTypesAnnotatedWith(NamedParameter.class));
    classes.addAll(r.getTypesAnnotatedWith(Unit.class));
    
    classes.addAll(r.getSubTypesOf(Name.class));
    
    JavaClassHierarchy ch = Tang.Factory.getTang().getDefaultClassHierarchy(jars, new Class[0]);
    for(Class<?> c : classes) {
      strings.add(ReflectionUtilities.getFullName(c));
    }
    for(String s : strings) {
      try {
        // Tang's unit tests include a lot of nasty corner cases; no need to look at those!
        if(checkTang || !s.startsWith("com.microsoft.tang")) {
//          System.out.println("Scanning " + s);
          ch.getNode(s);
        }
      } catch(ClassHierarchyException | NameResolutionException e) {
        System.err.println(e.getMessage());
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    new Tint();
  }

}
