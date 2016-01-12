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

import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.JavaClassHierarchy;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.*;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.walk.AbstractClassHierarchyNodeVisitor;
import org.apache.reef.tang.util.walk.NodeVisitor;
import org.apache.reef.tang.util.walk.Walk;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import javax.inject.Inject;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Tang Static Analytics Tool.
 */
public class Tint {
  private static final String SETTERS = "setters";
  private static final String USES = "uses";
  private static final String FULLNAME = "fullName";
  private final JavaClassHierarchy ch;
  private final Map<Field, ConfigurationModule> modules = new MonotonicHashMap<>();
  private final MonotonicMultiMap<String, String> setters = new MonotonicMultiMap<>();
  // map from thing that was used to user of the thing.
  private final MonotonicMultiMap<String, String> usages = new MonotonicMultiMap<>();
  private final Set<ClassNode<?>> knownClasses = new MonotonicSet<>();
  private final Set<String> divs = new MonotonicSet<>();

  private static final Logger LOG = Logger.getLogger(Tint.class.getName());

  {
    divs.add("doc");
    divs.add(USES);
    divs.add(SETTERS);

  }

  public Tint() {
    this(new URL[0]);
  }

  public Tint(final URL[] jars) {
    this(jars, false);
  }

  @SuppressWarnings("unchecked")
  public Tint(final URL[] jars, final boolean checkTang) {
    final Object[] args = new Object[jars.length + 6];
    for (int i = 0; i < jars.length; i++) {
      args[i] = jars[i];
    }
    args[args.length - 1] = new TypeAnnotationsScanner();
    args[args.length - 2] = new SubTypesScanner();
    args[args.length - 3] = new MethodAnnotationsScanner();
    args[args.length - 4] = new MethodParameterScanner();
    args[args.length - 5] = "com.microsoft";
    args[args.length - 6] = "org.apache";
    final Reflections r = new Reflections(args);
//    Set<Class<?>> classes = new MonotonicSet<>();
    final Set<String> strings = new TreeSet<>();
    final Set<String> moduleBuilders = new MonotonicSet<>();

    // Workaround bug in Reflections by keeping things stringly typed, and using Tang to parse them.
//  Set<Constructor<?>> injectConstructors = (Set<Constructor<?>>)(Set)r.getMethodsAnnotatedWith(Inject.class);
//  for(Constructor<?> c : injectConstructors) {
//    classes.add(c.getDeclaringClass());
//  }
    final Set<String> injectConstructors =
        r.getStore().getConstructorsAnnotatedWith(ReflectionUtilities.getFullName(Inject.class));
    for (final String s : injectConstructors) {
      strings.add(s.replaceAll("\\.<.+$", ""));
    }
    final Set<String> parameterConstructors =
        r.getStore().get(MethodParameterScanner.class, ReflectionUtilities.getFullName(Parameter.class));
    for (final String s : parameterConstructors) {
      strings.add(s.replaceAll("\\.<.+$", ""));
    }
//    Set<Class> r.getConstructorsWithAnyParamAnnotated(Parameter.class);
//    for(Constructor<?> c : parameterConstructors) {
//      classes.add(c.getDeclaringClass());
//    }
    final Set<String> defaultStrings =
        r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(DefaultImplementation.class));
    strings.addAll(defaultStrings);
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class,
        ReflectionUtilities.getFullName(NamedParameter.class)));
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(Unit.class)));
//    classes.addAll(r.getTypesAnnotatedWith(DefaultImplementation.class));
//    classes.addAll(r.getTypesAnnotatedWith(NamedParameter.class));
//    classes.addAll(r.getTypesAnnotatedWith(Unit.class));

    strings.addAll(r.getStore().get(SubTypesScanner.class, ReflectionUtilities.getFullName(Name.class)));

    moduleBuilders.addAll(r.getStore().get(SubTypesScanner.class,
        ReflectionUtilities.getFullName(ConfigurationModuleBuilder.class)));
//    classes.addAll(r.getSubTypesOf(Name.class));

    ch = Tang.Factory.getTang().getDefaultClassHierarchy(jars,
        (Class<? extends ExternalConstructor<?>>[]) new Class[0]);
//    for(String s : defaultStrings) {
//      if(classFilter(checkTang, s)) {
//        try {
//          ch.getNode(s);
//        } catch(ClassHierarchyException | NameResolutionException | ClassNotFoundException e) {
//          System.err.println(e.getMessage());
//        }
//      }
//    }

    for (final String s : strings) {
      if (classFilter(checkTang, s)) {
        try {
          ch.getNode(s);
        } catch (ClassHierarchyException | NameResolutionException e) {
          System.err.println(e.getMessage());
        }
      }
    }
    for (final String s : moduleBuilders) {
      if (classFilter(checkTang, s)) {
        try {
          ch.getNode(s);
        } catch (ClassHierarchyException | NameResolutionException e) {
          e.printStackTrace();
        }
      }
    }

    final NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {

      @Override
      public boolean visit(final NamedParameterNode<?> node) {
        final String nodeS = node.getFullName();
        for (final String s : node.getDefaultInstanceAsStrings()) {
          if (!usages.contains(s, nodeS)) {
            usages.put(s, nodeS);
          }
        }
        return true;
      }

      @Override
      public boolean visit(final PackageNode node) {
        return true;
      }

      @Override
      public boolean visit(final ClassNode<?> node) {
        final String nodeS = node.getFullName();
        for (final ConstructorDef<?> d : node.getInjectableConstructors()) {
          for (final ConstructorArg a : d.getArgs()) {
            if (a.getNamedParameterName() != null &&
                !usages.contains(a.getNamedParameterName(), nodeS)) {
              usages.put(a.getNamedParameterName(), nodeS);
            }
          }
        }
        if (!knownClasses.contains(node)) {
          knownClasses.add(node);
        }
        return true;
      }
    };
    int numClasses;
    do {
      numClasses = knownClasses.size();

      Walk.preorder(v, null, ch.getNamespace());

      for (final ClassNode<?> cn : knownClasses) {
        try {
          final String s = cn.getFullName();
          if (classFilter(checkTang, s)) {
            final Class<?> c = ch.classForName(s);
            processDefaultAnnotation(c);
            processConfigurationModules(c);
          }
        } catch (final ClassNotFoundException e) {
          e.printStackTrace();
        }
      }

      for (final Entry<Field, ConfigurationModule> entry: modules.entrySet()) {
        final String fS = ReflectionUtilities.getFullName(entry.getKey());
        final Set<NamedParameterNode<?>> nps = entry.getValue().getBoundNamedParameters();
        for (final NamedParameterNode<?> np : nps) {
          final String npS = np.getFullName();
          if (!setters.contains(npS, fS)) {
            setters.put(npS, fS);
          }
        }
      }
    } while (numClasses != knownClasses.size()); // Note naive fixed point evaluation here.  Semi-naive would be faster.

  }

  public static String stripCommonPrefixes(final String s) {
    return
        stripPrefixHelper2(
            stripPrefixHelper2(s, "java.lang"),
            "java.util");
  }

  public static String stripPrefixHelper2(final String s, final String prefix) {
    final String[] pretok = prefix.split("\\.");
    final String[] stok = s.split("\\.");
    final StringBuffer sb = new StringBuffer();
    int i;
    for (i = 0; i < pretok.length && i < stok.length; i++) {
      if (pretok[i].equals(stok[i])) {
        sb.append(pretok[i].charAt(0) + ".");
      } else {
        break;
      }
    }
    for (; i < stok.length - 1; i++) {
      sb.append(stok[i] + ".");
    }
    sb.append(stok[stok.length - 1]);
    return sb.toString();
  }

  /*  public static String stripPrefixHelper(String s, String prefix) {
      if(!"".equals(prefix) && s.startsWith(prefix+".")) {
        try {
          String suffix = s.substring(prefix.length()+1);
          if(suffix.matches("^[A-Z].+")){
            return suffix;
          } else {
            String shorten = prefix.replaceAll("([^.])[^.]+", "$1");
            return shorten + "." + suffix;
          }
        } catch(StringIndexOutOfBoundsException e) {
          throw new RuntimeException("Couldn't strip " + prefix + " from " + s, e);
        }
      } else {
        return s;
      }
    }*/
  public static String stripPrefix(final String s, final String prefix) {
    return stripPrefixHelper2(stripPrefixHelper2(stripPrefixHelper2(
        stripCommonPrefixes(stripPrefixHelper2(s, prefix)),
        "org.apache.reef"), "org.apache.reef"), "org.apache.reef.wake");
  }

  /**
   * @param args
   * @throws FileNotFoundException
   * @throws MalformedURLException
   */
  public static void main(final String[] args)
      throws FileNotFoundException, MalformedURLException, UnsupportedEncodingException {
    int i = 0;
    String doc = null;
    String jar = null;
    boolean tangTests = false;
    while (i < args.length) {
      if (args[i].equals("--doc")) {
        i++;
        doc = args[i];
      } else if (args[i].equals("--jar")) {
        i++;
        jar = args[i];
      } else if (args[i].equals("--tang-tests")) {
        tangTests = true;
      }

      i++;
    }

    final Tint t;
    if (jar != null) {
      final File f = new File(jar);
      if (!f.exists()) {
        throw new FileNotFoundException(jar);
      }
      t = new Tint(new URL[]{f.toURI().toURL()}, tangTests);
    } else {
      t = new Tint(new URL[0], tangTests);
    }

    if (doc != null) {
      try (final PrintStream out = new PrintStream(doc, "UTF-8")) {
        out.println("<html><head><title>TangDoc</title>");

        out.println("<style>");
        out.println("body { font-family: 'Segoe UI', 'Helvetica'; font-size:12pt; font-weight: 200; " +
            "margin: 1em; column-count: 2; }");
        out.println(".package { font-size:18pt; font-weight: 500; column-span: all; }");
//      out.println(".class { break-after: never; }");
//      out.println(".doc { break-before: never; }");
        out.println(".decl-margin { padding: 8pt; break-inside: avoid; }");
        out.println(".module-margin { padding: 8pt; column-span: all; break-inside: avoid; }");
        out.println(".decl { background-color: aliceblue; padding: 6pt;}");
        out.println(".fullName { font-size: 11pt; font-weight: 400; }");
        out.println(".simpleName { font-size: 11pt; font-weight: 400; }");
        out.println(".constructorArg { padding-left: 16pt; }");
        out.println("." + SETTERS + " { padding-top: 6pt; font-size: 10pt; }");
        out.println("." + USES + " { padding-top: 6pt; font-size: 10pt; }");
        out.println("pre { font-size: 10pt; }");
        out.println("</style>");

        out.println("</head><body>");

        String currentPackage = "";
        for (final Node n : t.getNamesUsedAndSet()) {
          final String fullName = n.getFullName();
          final String[] tok = fullName.split("\\.");
          final StringBuffer sb = new StringBuffer(tok[0]);
          for (int j = 1; j < tok.length; j++) {
            if (tok[j].matches("^[A-Z].*") || j > 5) {
              break;
            } else {
              sb.append("." + tok[j]);
            }
          }
          final String pack = sb.toString();
          if (!currentPackage.equals(pack)) {
            currentPackage = pack;
            out.println(t.endPackage());
            out.println(t.startPackage(currentPackage));
          }
          if (n instanceof NamedParameterNode<?>) {
            out.println(t.toHtmlString((NamedParameterNode<?>) n, currentPackage));
          } else if (n instanceof ClassNode<?>) {
            out.println(t.toHtmlString((ClassNode<?>) n, currentPackage));
          } else {
            throw new IllegalStateException();
          }
        }
        out.println("</div>");
        out.println(t.endPackage());
        out.println("<div class='package'>Module definitions</div>");
        for (final Field f : t.modules.keySet()) {
          final String moduleName = ReflectionUtilities.getFullName(f);
//        String declaringClassName = ReflectionUtilities.getFullName(f.getDeclaringClass());
          out.println("<div class='module-margin' id='" + moduleName + "'><div class='decl'><span class='fullName'>" +
              moduleName + "</span>");
          out.println("<pre>");
          final String conf = t.modules.get(f).toPrettyString();
          final String[] tok = conf.split("\n");
          for (final String line : tok) {
            out.println(stripPrefix(line, "no.such.prefix")); //t.modules.get(f).toPrettyString());
          }
//        List<Entry<String,String>> lines = t.modules.get(f).toStringPairs();
//        for(Entry<String,String> line : lines) {
//          String k = t.stripPrefix(line.getKey(), declaringClassName);
//          String v = t.stripPrefix(line.getValue(), declaringClassName);
//          out.println(k+"="+v);
//        }
          out.println("</pre>");
          out.println("</div></div>");
        }

        out.println("<div class='package'>Interfaces and injectable classes</div>");
        for (final ClassNode<?> c : t.knownClasses) {
          if (t.classFilter(tangTests, c.getFullName())) {
            Class<?> clz = null;
            try {
              clz = t.ch.classForName(c.getFullName());
            } catch (final ClassNotFoundException e) {
              // TODO[JIRA REEF-864] Clarify handling in this case
              e.printStackTrace();
            }
            final String typ = clz == null ? "undefined" : clz.isInterface() ? "interface" : "class";
            out.println("<div class='module-margin' id='" + c.getFullName() + "'><div class='decl'>" +
                "<span class='fullName'>" + typ + " " + c.getFullName() + "</span>");
            for (final ConstructorDef<?> d : c.getInjectableConstructors()) {
              out.println("<div class='uses'>" + c.getFullName() + "(");
              for (final ConstructorArg a : d.getArgs()) {
                if (a.getNamedParameterName() != null) {
                  out.print("<div class='constructorArg'><a href='#" + a.getType() + "'>" +
                      stripPrefix(a.getType(), "xxx") + "</a> <a href='#" + a.getNamedParameterName() + "'>" +
                      a.getNamedParameterName() + "</a></div>");
                } else {
                  out.print("<div class='constructorArg'><a href='#" + a.getType() + "'>" +
                      stripPrefix(a.getType(), "xxx") + "</a></div>");
                }
              }
              out.println(")</div>");
            }
            out.println("</div></div>");
          }
/*
      out.println("<h1>Default usage of classes and constants</h1>");
      for(String s : t.usages.keySet()) {
        out.println("<h2>" + s + "</h2>");
        for(Node n : t.usages.getValuesForKey(s)) {
          out.println("<p>" + n.getFullName() + "</p>");
        }
      } */
        }
        out.println("</body></html>");
      }
    }
  }

  private boolean classFilter(final boolean checkTang, final String s) {
    return checkTang || !s.startsWith("org.apache.reef.tang");
  }

  private void processDefaultAnnotation(final Class<?> cmb) {
    final DefaultImplementation di = cmb.getAnnotation(DefaultImplementation.class);
    // XXX hack: move to helper method + unify with rest of Tang!
    if (di != null) {
      final String diName = di.value() == Void.class ? di.name() : ReflectionUtilities.getFullName(di.value());
      final ClassNode<?> cn = (ClassNode<?>) ch.getNode(cmb);
      final String cnS = cn.getFullName();
      if (!usages.contains(diName, cnS)) {
        usages.put(diName, cnS);
        if (!knownClasses.contains(cn)) {
          knownClasses.add(cn);
        }
      }
    }
  }

  private void processConfigurationModules(final Class<?> cmb) {
    for (final Field f : cmb.getFields()) {
      if (ReflectionUtilities.isCoercable(ConfigurationModule.class, f.getType())) {
        final int mod = f.getModifiers();
        boolean ok = true;
        if (Modifier.isPrivate(mod)) {
          System.err.println("Found private ConfigurationModule " + f);
          ok = false;
        }
        if (!Modifier.isFinal(mod)) {
          System.err.println("Found non-final ConfigurationModule " + f);
          ok = false;
        }
        if (!Modifier.isStatic(f.getModifiers())) {
          System.err.println("Found non-static ConfigurationModule " + f);
          ok = false;
        }
        if (ok) {
//          System.err.println("OK: " + f);
          try {
            f.setAccessible(true);
            final String fS = ReflectionUtilities.getFullName(f);
            if (!modules.containsKey(f)) {
              modules.put(f, (ConfigurationModule) (f.get(null)));
              try {
                modules.get(f).assertStaticClean();
              } catch (final ClassHierarchyException e) {
                System.err.println(fS + ": " + e.getMessage());
              }
              for (final Entry<String, String> e : modules.get(f).toStringPairs()) {
                //System.err.println("e: " + e.getKey() + "=" + e.getValue());
                try {
                  final Node n = ch.getNode(e.getKey());
                  if (!setters.contains(e.getKey(), fS)) {
                    setters.put(e.getKey(), fS);
                  }
                  if (n instanceof ClassNode) {
                    final ClassNode<?> cn = (ClassNode<?>) n;
                    if (!knownClasses.contains(cn)) {
                      knownClasses.add(cn);
                    }
                  }
                } catch (final NameResolutionException ex) {
                  LOG.warning("The class " + e.getValue() + " not found in the class hierarchy."
                          + " The exception message: " + ex.getMessage());
                }
                try {
                  final String s = e.getValue();
                  final Node n = ch.getNode(s);
                  if (!usages.contains(ReflectionUtilities.getFullName(f), s)) {
                    //  System.err.println("Added usage: " + ReflectionUtilities.getFullName(f) + "=" + s);
                    usages.put(s, ReflectionUtilities.getFullName(f));
                  }
                  if (n instanceof ClassNode) {
                    final ClassNode<?> cn = (ClassNode<?>) n;
                    if (!knownClasses.contains(cn)) {
                      System.err.println("Added " + cn + " to known classes");
                      knownClasses.add(cn);
                    }
                  }
                } catch (final NameResolutionException ex) {
                  LOG.warning("The class " + e.getValue() + " not found in the class hierarchy."
                               + " The exception message: " + ex.getMessage());
                }
              }
            }
          } catch (final ExceptionInInitializerError e) {
            System.err.println("Field " + ReflectionUtilities.getFullName(f) + ": " + e.getCause().getMessage());
          } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public Set<NamedParameterNode<?>> getNames() {
    final Set<NamedParameterNode<?>> names = new MonotonicSet<>();
    final NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {

      @Override
      public boolean visit(final NamedParameterNode<?> node) {
        names.add(node);
        return true;
      }

      @Override
      public boolean visit(final PackageNode node) {
        return true;
      }

      @Override
      public boolean visit(final ClassNode<?> node) {
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return names;
  }

  public Set<Node> getNamesUsedAndSet() {
    final Set<Node> names = new MonotonicSet<>();
    final Set<String> userKeys = usages.keySet();
    final Set<String> usedKeys = usages.values();
    final Set<String> setterKeys = setters.keySet();
    final NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {

      @Override
      public boolean visit(final NamedParameterNode<?> node) {
        names.add(node);
        return true;
      }

      @Override
      public boolean visit(final PackageNode node) {
        return true;
      }

      @Override
      public boolean visit(final ClassNode<?> node) {
        final String nodeS = node.getFullName();
        if (userKeys.contains(nodeS)) {
          names.add(node);
        }
        if (setterKeys.contains(nodeS)) {
          names.add(node);
        }
        if (usedKeys.contains(nodeS) && !names.contains(node)) {
          names.add(node);
        }

        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return names;
  }

  public Set<String> getUsesOf(final Node name) {

    return usages.getValuesForKey(name.getFullName());
  }

  public Set<String> getSettersOf(final Node name) {
    return setters.getValuesForKey(name.getFullName());
  }

  public String toString(final NamedParameterNode<?> n) {
    final StringBuilder sb = new StringBuilder("Name: " + n.getSimpleArgName() + " " + n.getFullName());
    final String[] instances = n.getDefaultInstanceAsStrings();
    if (instances.length != 0) {
      sb.append(" = ");
      sb.append(join(",", instances));
    }
    if (!n.getDocumentation().equals("")) {
      sb.append(" //" + n.getDocumentation());
    }
    return sb.toString();
  }

  private String join(final String sep, final String[] s) {
    if (s.length > 0) {
      final StringBuffer sb = new StringBuffer(s[0]);
      for (int i = 1; i < s.length; i++) {
        sb.append(sep);
        sb.append(s[i]);
      }
      return sb.toString();
    } else {
      return null;
    }
  }

  public String cell(final String s, final String clazz) {
    String str = s;
    if (clazz.equals(USES) && str.length() > 0) {
      str = "<em>Used by:</em><br>" + str;
    }
    if (clazz.equals(SETTERS) && str.length() > 0) {
      str = "<em>Set by:</em><br>" + str;
    }
    if (clazz.equals(FULLNAME)) {
      str = "&nbsp;" + str;
    }
    if (divs.contains(clazz)) {
      return "<div class=\"" + clazz + "\">" + str + "</div>";
    } else {
      return "<span class=\"" + clazz + "\">" + str + "</span>";
    }
  }

  public String cell(final StringBuffer sb, final String clazz) {
    return cell(sb.toString(), clazz);
  }

  public String row(final StringBuffer sb) {
    return sb.toString();
  }

  public String toHtmlString(final NamedParameterNode<?> n, final String pack) {
    final String fullName = stripPrefix(n.getFullName(), pack);
    final StringBuffer sb = new StringBuffer();

    sb.append("<div id='" + n.getFullName() + "' class='decl-margin'>");
    sb.append("<div class='decl'>");

    sb.append(cell(n.getSimpleArgName(), "simpleName") + cell(fullName, FULLNAME));
    final String instance;
    final String[] instances = n.getDefaultInstanceAsStrings();
    if (instances.length != 0) {
      final StringBuffer sb2 = new StringBuffer(" = " + stripPrefix(instances[0], pack));
      for (int i = 1; i < instances.length; i++) {
        sb2.append("," + stripPrefix(instances[i], pack));
      }
      instance = sb2.toString();
    } else {
      instance = "";
    }
    sb.append(cell(instance, "instance"));
    final StringBuffer doc = new StringBuffer();
    if (!n.getDocumentation().equals("")) {
      doc.append(n.getDocumentation());
    }
    sb.append(cell(doc, "doc"));
    final StringBuffer uses = new StringBuffer();
    for (final String u : getUsesOf(n)) {
      uses.append("<a href='#" + u + "'>" + stripPrefix(u, pack) + "</a> ");
    }
    sb.append(cell(uses, USES));
    final StringBuffer settersStr = new StringBuffer();
    for (final String f : getSettersOf(n)) {
      settersStr.append("<a href='#" + f + "'>" + stripPrefix(f, pack) + "</a> ");
    }
    sb.append(cell(settersStr, SETTERS));
    sb.append("</div>");
    sb.append("</div>");
    return row(sb);
  }

  public String toHtmlString(final ClassNode<?> n, final String pack) {
    final String fullName = stripPrefix(n.getFullName(), pack);

    final String type;
    try {
      if (ch.classForName(n.getFullName()).isInterface()) {
        type = "interface";
      } else {
        type = "class";
      }
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    final StringBuffer sb = new StringBuffer();

    sb.append("<div class='decl-margin' id='" + n.getFullName() + "'>");
    sb.append("<div class='decl'>");
    sb.append(cell(type, "simpleName") + cell(fullName, FULLNAME));
    final String instance;
    if (n.getDefaultImplementation() != null) {
      instance = " = " + stripPrefix(n.getDefaultImplementation(), pack);
    } else {
      instance = "";
    }
    sb.append(cell(instance, "simpleName"));
    sb.append(cell("", "fullName")); // TODO[REEF-1118]: Support documentation string
    final StringBuffer uses = new StringBuffer();
    for (final String u : getUsesOf(n)) {
      uses.append("<a href='#" + u + "'>" + stripPrefix(u, pack) + "</a> ");
    }
    sb.append(cell(uses, USES));
    final StringBuffer settersStr = new StringBuffer();
    for (final String f : getSettersOf(n)) {
      settersStr.append("<a href='#" + f + "'>" + stripPrefix(f, pack) + "</a> ");
    }
    sb.append(cell(settersStr, SETTERS));
    sb.append("</div>");
    sb.append("</div>");
    return row(sb);
  }

  public String startPackage(final String pack) {
    return "<div class=\"package\">" + pack + "</div>";
//    return "<tr><td colspan='6'><br><b>" + pack + "</b></td></tr>";
  }

  public String endPackage() {
    return "";
  }
}
