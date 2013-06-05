package com.microsoft.tang;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestImplicitConversions {
  static interface Identifier {
    
  };
  static interface AIdentifier extends Identifier {
    
  }
  static class AIdentifierImpl implements AIdentifier {
    private final String aString;

    @Inject
    AIdentifierImpl(String aString) {
      this.aString = aString;
    }
    @Override
    public String toString() { return aString; }
  };
  static interface BIdentifier extends Identifier {
    
  }
  static class BIdentifierImpl implements BIdentifier {
    private final String bString;

    @Inject
    BIdentifierImpl(String bString) {
      this.bString = bString;
    }
    @Override
    public String toString() { return bString; }
  };
  static class IdentifierParser implements ExternalConstructor<Identifier> {
    final Identifier id;
    @Inject
    public IdentifierParser(String id) {
      this.id = id.startsWith("a://") ? new BIdentifierImpl(id) : id.startsWith("b://") ? new BIdentifierImpl(id) : null;
      if(this.id == null) {
        throw new IllegalArgumentException("Need string that starts with a:// or b://!");
      }
    }
    @Override
    public Identifier newInstance() {
      return id;
    }
  }
  @NamedParameter
  class IdName implements Name<Identifier> {}
  @NamedParameter
  class AIdName implements Name<AIdentifier> {}
  @NamedParameter
  class BIdName implements Name<BIdentifier> {}
  
  @SuppressWarnings("unchecked")
  @Test
  public void testBindFromString() throws BindException, InjectionException {
    JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    b.bindNamedParameter(IdName.class, "b://b"); //new BIdentifierImpl("b://b"));
    
    Configuration c = b.build();
    String s = ConfigurationFile.toConfigurationString(c);
    
    JavaConfigurationBuilder b2 = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    // BUG: This line fails!  b.bindParser(Identifier.class, IdentifierParser.class);
    ConfigurationFile.addConfiguration(b2, s);
    Configuration c2 =  b2.build();
    
    Assert.assertEquals("b://b", c2.getNamedParameter((NamedParameterNode<?>)c2.getClassHierarchy().getNode(ReflectionUtilities.getFullName(IdName.class))));
    Injector i = Tang.Factory.getTang().newInjector(c2);
    
    Assert.assertEquals("b://b", i.getNamedInstance(IdName.class).toString());
    
  }
  @SuppressWarnings("unchecked")
  @Test
  public void testBindSubclassFromString() throws BindException, InjectionException {
    JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    b.bindNamedParameter(BIdName.class, "b://b"); //new BIdentifierImpl("b://b"));
    
    Configuration c = b.build();
    String s = ConfigurationFile.toConfigurationString(c);
    
    JavaConfigurationBuilder b2 = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    // BUG: This line fails!  b.bindParser(Identifier.class, IdentifierParser.class);
    ConfigurationFile.addConfiguration(b2, s);
    Configuration c2 =  b2.build();
    
    Assert.assertEquals("b://b", c2.getNamedParameter((NamedParameterNode<?>)c2.getClassHierarchy().getNode(ReflectionUtilities.getFullName(BIdName.class))));
    Injector i = Tang.Factory.getTang().newInjector(c2);
    
    Assert.assertEquals("b://b", i.getNamedInstance(BIdName.class).toString());
    
  }
  
}
