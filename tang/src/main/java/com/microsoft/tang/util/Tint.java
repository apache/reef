package com.microsoft.tang.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.walk.AbstractClassHierarchyNodeVisitor;
import com.microsoft.tang.util.walk.NodeVisitor;
import com.microsoft.tang.util.walk.Walk;

public class Tint {
  final JavaClassHierarchy ch;
  final Map<Field, ConfigurationModule> modules = new MonotonicHashMap<>();
  final MonotonicMultiMap<Node, Field> setters= new MonotonicMultiMap<>();
  // map from user to thing that was used.
  final MonotonicMultiMap<String, Node> usages= new MonotonicMultiMap<>();
  
  public Tint() {
    this(new URL[0]);
  }
  public Tint(URL[] jars) {
    this(jars, false);
  }

  public Tint(URL[] jars, boolean checkTang) {
    Object[] args = new Object[jars.length + 6];
    for(int i = 0; i < jars.length; i++){
      args[i] = jars[i];
    }
    args[args.length-1] = new TypeAnnotationsScanner();
    args[args.length-2] = new SubTypesScanner();
    args[args.length-3] = new MethodAnnotationsScanner();
    args[args.length-4] = new MethodParameterScanner();
    args[args.length-5] = "com.microsoft";
    args[args.length-6] = "org.apache";
    Reflections r = new Reflections(args);
    Set<Class<?>> classes = new MonotonicSet<>();
    Set<String> strings = new TreeSet<>();
    Set<String> moduleBuilders = new MonotonicSet<>();

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
    Set<String> defaultStrings = r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(DefaultImplementation.class));
    strings.addAll(defaultStrings);
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(NamedParameter.class)));
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(Unit.class)));
//    classes.addAll(r.getTypesAnnotatedWith(DefaultImplementation.class));
//    classes.addAll(r.getTypesAnnotatedWith(NamedParameter.class));
//    classes.addAll(r.getTypesAnnotatedWith(Unit.class));
    
    strings.addAll(r.getStore().get(SubTypesScanner.class, ReflectionUtilities.getFullName(Name.class)));
    
    moduleBuilders.addAll(r.getStore().get(SubTypesScanner.class, ReflectionUtilities.getFullName(ConfigurationModuleBuilder.class)));
//    classes.addAll(r.getSubTypesOf(Name.class));

    ch = Tang.Factory.getTang().getDefaultClassHierarchy(jars, new Class[0]);
    for(String s : defaultStrings) {
      try {
        if(checkTang || !s.startsWith("com.microsoft.tang")) {
        try {
          DefaultImplementation di = ch.classForName(s).getAnnotation(DefaultImplementation.class);
          // XXX hack: move to helper method + unify with rest of Tang!
          String diName = di.value() == Void.class ? di.name() : ReflectionUtilities.getFullName(di.value());
          strings.add(diName);
              usages.put(diName, ch.getNode(s));
          } catch(ClassHierarchyException | NameResolutionException e) {
            System.err.println(e.getMessage());
          }
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    
    for(String s : strings) {
      try {
        // Tang's unit tests include a lot of nasty corner cases; no need to look at those!
        if(checkTang || !s.startsWith("com.microsoft.tang")) {
          ch.getNode(s);
        }
      } catch(ClassHierarchyException | NameResolutionException e) {
        System.err.println(e.getMessage());
      }
    }
    for(String mb : moduleBuilders) {
      if(checkTang || !mb.startsWith("com.microsoft.tang")) {
        try {
          @SuppressWarnings("unchecked")
          Class<ConfigurationModuleBuilder> cmb = (Class<ConfigurationModuleBuilder>) ch.classForName(mb);
          for(Field f : cmb.getFields()) {
            if(ReflectionUtilities.isCoercable(ConfigurationModule.class, f.getType())) {
  
              int mod = f.getModifiers();
              if(!Modifier.isPrivate(mod)) {
                if(!Modifier.isFinal(mod)) {
                  System.err.println("Style warning: Found non-final ConfigurationModule" + f);
                }
                if(!Modifier.isStatic(f.getModifiers())) {
                  System.err.println("Style warning: Found non-static ConfigurationModule " + f);
                } else {
                  try {
                    f.setAccessible(true);
                    modules.put(f,(ConfigurationModule)(f.get(null)));
                    for(Entry<String, String> e : modules.get(f).toStringPairs()) {
                      try {
                        Node n = ch.getNode(e.getKey());
                        /// XXX would be great if this referred to the field in question (not the conf)
                        setters.put(n,f);
                      } catch(NameResolutionException ex) {
                        
                      }
                      try {
                        Node n = ch.getNode(e.getValue());
                        /// XXX would be great if this referred to the field in question (not the conf)
                        usages.put(ReflectionUtilities.getFullName(f),n);
                      } catch(NameResolutionException ex) {
                        
                      }
                    }
                  } catch(ExceptionInInitializerError e) {
                    System.err.println("Field " + ReflectionUtilities.getFullName(f) + ": " + e.getCause().getMessage());
                  } catch(IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                }
              }
            }
          }
        } catch(ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    }
    for(Class<?> c : classes) {
      strings.add(ReflectionUtilities.getFullName(c));
    }

    NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {
      
      @Override
      public boolean visit(NamedParameterNode<?> node) {
        if(node.getDefaultInstanceAsString() != null &&
            !usages.contains(node.getDefaultInstanceAsString(), node)) {
          usages.put(node.getDefaultInstanceAsString(), node);
        }
        return true;
      }
      
      @Override
      public boolean visit(PackageNode node) {
        return true;
      }
      
      @Override
      public boolean visit(ClassNode<?> node) {
        for(ConstructorDef<?> d : node.getInjectableConstructors()) {
          for(ConstructorArg a : d.getArgs()) {
            if(a.getNamedParameterName() != null &&
                !usages.contains(a.getNamedParameterName(), node)) {
              usages.put(a.getNamedParameterName(), node);
            }
          }
        }
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());

    for(Field f : modules.keySet()) {
      ConfigurationModule m = modules.get(f);
      Set<NamedParameterNode<?>> nps = m.getBoundNamedParameters();
      for(NamedParameterNode<?> np : nps) {
        if(!setters.contains(np, f)) {
          setters.put(np, f);
        }
      }
    }
  }
  
  public Set<NamedParameterNode<?>> getNames() {
    final Set<NamedParameterNode<?>> names = new MonotonicSet<>();
    NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {
      
      @Override
      public boolean visit(NamedParameterNode<?> node) {
        names.add(node);
        return true;
      }
      
      @Override
      public boolean visit(PackageNode node) {
        return true;
      }
      
      @Override
      public boolean visit(ClassNode<?> node) {
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return names;
  }

  public Set<Node> getNamesUsedAndSet() {
    final Set<Node> names = new MonotonicSet<>();
    final Set<String> usedKeys = usages.keySet();
    final Set<Node> setterKeys = setters.keySet();
    NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {

      @Override
      public boolean visit(NamedParameterNode<?> node) {
        names.add(node);
        return true;
      }
      
      @Override
      public boolean visit(PackageNode node) {
        return true;
      }
      
      @Override
      public boolean visit(ClassNode<?> node) {
        if(usedKeys.contains(node.getFullName())) {
          names.add(node);
        }
        if(setterKeys.contains(node)) {
          names.add(node);
        }
        
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return names;
  }

  public Set<Node> getUsesOf(final Node name) {

    return usages.getValuesForKey(name.getFullName());
  }
  public Set<Field> getSettersOf(final Node name) {
    return setters.getValuesForKey(name);
  }
  public String stripCommonPrefixes(String s) {
    return 
        stripPrefixHelper(
        stripPrefixHelper(s, "java.lang"),
        "java.util");
  }
  public String stripPrefixHelper(String s, String prefix) {
    if(!"".equals(prefix) && s.startsWith(prefix)) {
      try {
        return s.substring(prefix.length()+1);
      } catch(StringIndexOutOfBoundsException e) {
        throw new RuntimeException("Couldn't strip " + prefix + " from " + s, e);
      }
    } else {
      return s;
    }
  }
  public String stripPrefix(String s, String prefix) {
    return stripCommonPrefixes(stripPrefixHelper(s,prefix));
  }
  public String toString(NamedParameterNode n) {
    StringBuilder sb = new StringBuilder("Name: " + n.getSimpleArgName() + " " + n.getFullName());
    if(n.getDefaultInstanceAsString() != null) {
      sb.append(" = " + n.getDefaultInstanceAsString());
    }
    if(!n.getDocumentation().equals("")) {
      sb.append(" //" + n.getDocumentation());
    }
    return sb.toString();
  }
  public String toHtmlString(NamedParameterNode<?> n, String pack) {
    final String sep = "</td><td>";
    String fullName = stripPrefix(n.getFullName(), pack);

    StringBuilder sb = new StringBuilder("<tr><td>" + n.getSimpleArgName() + sep + fullName + sep);
    if(n.getDefaultInstanceAsString() != null) {
      String instance = stripPrefix(n.getDefaultInstanceAsString(), pack);
      
      sb.append(instance);
    }
    sb.append(sep);
    if(!n.getDocumentation().equals("")) {
      sb.append(n.getDocumentation());
    }
    Set<Node> uses = getUsesOf(n);
    sb.append(sep);
    for(Node u : uses) {
      sb.append(stripPrefix(u.getFullName(), pack) + "<br>");
    }
    sb.append(sep);
    for(Field f : getSettersOf(n)) {
      sb.append(stripPrefix(ReflectionUtilities.getFullName(f), pack) + "<br>");
    }
    sb.append("</td></tr>");
    return sb.toString();
  }
  public String toHtmlString(ClassNode<?> n, String pack) {
    final String sep = "</td><td>";
    String fullName = stripPrefix(n.getFullName(), pack);
    
    final String type;
    try {
      if(ch.classForName(n.getFullName()).isInterface()) {
        type = "interface";
      } else {
        type = "class";
      }
    } catch(ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    StringBuilder sb = new StringBuilder("<tr><td>" + type + sep + fullName + sep);
    if(n.getDefaultImplementation() != null) {
      String instance = stripPrefix(n.getDefaultImplementation(), pack);
      
      sb.append(instance);
    }
    sb.append(sep);
    sb.append(""); // TODO: Documentation string?

    Set<Node> uses = getUsesOf(n);
    sb.append(sep);
    for(Node u : uses) {
      sb.append(stripPrefix(u.getFullName(), pack) + "<br>");
    }
    sb.append(sep);
    for(Field f : getSettersOf(n)) {
      sb.append(stripPrefix(ReflectionUtilities.getFullName(f), pack) + "<br>");
    }
    sb.append("</td></tr>");
    return sb.toString();
  }
  public String startPackage(String pack) {
    return "<tr><td colspan='5'><br><b>" + pack + "</b></td></tr>";
  }
  public String endPackage() {
    return "";
  }
  
  /**
   * @param args
   * @throws FileNotFoundException 
   * @throws MalformedURLException 
   */
  public static void main(String[] args) throws FileNotFoundException, MalformedURLException {
    int i = 0;
    String doc = null;
    String jar = null;
    boolean tangTests = false;
    while(i < args.length) {
      if(args[i].equals("--doc")) {
        i++;
        doc = args[i];
      } else if(args[i].equals("--jar")) {
        i++;
        jar = args[i];
      } else if(args[i].equals("--tang-tests")) {
        tangTests = true;
      }
      
      i++;
    }
    final Tint t;
    if(jar != null) {
      File f = new File(jar);
      if(!f.exists()) { throw new FileNotFoundException(jar); }
      t = new Tint(new URL[] { f.toURI().toURL() }, tangTests);
    } else {
      t = new Tint(new URL[0], tangTests);
    }
    if(doc != null) {
      PrintStream out = new PrintStream(new FileOutputStream(new File(doc)));
      out.println("<html><head><title>TangDoc</title></head><body>");
      out.println("<table border='1'><tr><th>Type</th><th>Name</th><th>Default value</th><th>Documentation</th><th>Used by</th><th>Set by</th></tr>");

      String currentPackage = "";
      for(Node n : t.getNamesUsedAndSet()) {
        String fullName = n.getFullName();
        String tok[] = fullName.split("\\.");
        StringBuffer sb = new StringBuffer(tok[0]);
        for(int j = 1; j < tok.length; j++) {
          if(tok[j].matches("^[A-Z].*") || j > 4) {
            break;
          } else
            sb.append("." + tok[j]);
        }
        String pack = sb.toString();
        if(!currentPackage.equals(pack)) {
          currentPackage = pack;
          out.println(t.endPackage());
          out.println(t.startPackage(currentPackage));
          
        }
        if(n instanceof NamedParameterNode<?>) {
          out.println(t.toHtmlString((NamedParameterNode<?>)n, currentPackage));
        } else if (n instanceof ClassNode<?>) {
          out.println(t.toHtmlString((ClassNode<?>)n, currentPackage));
        } else {
          throw new IllegalStateException();
        }
      }
      out.println(t.endPackage());
      out.println("</table>");
      out.println("<h1>Module definitions</h1>");
      for(Field f : t.modules.keySet()) {
        String moduleName = ReflectionUtilities.getFullName(f);
        String declaringClassName = ReflectionUtilities.getFullName(f.getDeclaringClass());
        out.println("<p><b>" + moduleName + "</b></p>");
        out.println("<pre>");
        out.println(t.modules.get(f).toPrettyString());
//        List<Entry<String,String>> lines = t.modules.get(f).toStringPairs();
//        for(Entry<String,String> line : lines) {
//          String k = t.stripPrefix(line.getKey(), declaringClassName);
//          String v = t.stripPrefix(line.getValue(), declaringClassName);
//          out.println(k+"="+v);
//        }
        out.println("</pre>");
      }

//      out.println("<h1>Default usage of classes and constants</h1>");
//      for(String s : t.usages.keySet()) {
//        out.println("<h2>" + s + "</h2>");
//        for(Node n : t.usages.getValuesForKey(s)) {
//          out.println("<p>" + n.getFullName() + "</p>");
//        }
//      }
      out.println("</body></html>");
      out.close();
      
    }
  }

}
