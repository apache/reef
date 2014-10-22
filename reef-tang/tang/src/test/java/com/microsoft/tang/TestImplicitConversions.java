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
package com.microsoft.tang;

import javax.inject.Inject;

import org.junit.Assert;

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
      this.id = id.startsWith("a://") ? new AIdentifierImpl(id) : id.startsWith("b://") ? new BIdentifierImpl(id) : null;
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
    b.bindNamedParameter(IdName.class, "b://b");
    
    Configuration c = b.build();
    String s = ConfigurationFile.toConfigurationString(c);
    
    JavaConfigurationBuilder b2 = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    ConfigurationFile.addConfiguration(b2, s);
    Configuration c2 =  b2.build();
    
    Assert.assertEquals("b://b", c2.getNamedParameter((NamedParameterNode<?>)c2.getClassHierarchy().getNode(ReflectionUtilities.getFullName(IdName.class))));
    Injector i = Tang.Factory.getTang().newInjector(c2);
    
    Assert.assertEquals("b://b", i.getNamedInstance(IdName.class).toString());
    Assert.assertTrue(i.getNamedInstance(IdName.class) instanceof BIdentifier);
    
  }
  @SuppressWarnings("unchecked")
  @Test
  public void testBindSubclassFromString() throws BindException, InjectionException {
    JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    b.bindNamedParameter(AIdName.class, "a://a");
    b.bindNamedParameter(BIdName.class, "b://b");
    
    Configuration c = b.build();
    String s = ConfigurationFile.toConfigurationString(c);
    
    JavaConfigurationBuilder b2 = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    ConfigurationFile.addConfiguration(b2, s);
    Configuration c2 =  b2.build();
    
    Assert.assertEquals("b://b", c2.getNamedParameter((NamedParameterNode<?>)c2.getClassHierarchy().getNode(ReflectionUtilities.getFullName(BIdName.class))));
    Injector i = Tang.Factory.getTang().newInjector(c2);
    
    Assert.assertEquals("b://b", i.getNamedInstance(BIdName.class).toString());
    Assert.assertTrue(i.getNamedInstance(BIdName.class) instanceof BIdentifier);
    Assert.assertEquals("a://a", i.getNamedInstance(AIdName.class).toString());
    Assert.assertTrue(i.getNamedInstance(AIdName.class) instanceof AIdentifier);
  }
  @SuppressWarnings("unchecked")
  @Test(expected=ClassCastException.class)
  public void testBindWrongSubclassFromString() throws BindException, InjectionException {
    JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    b.bindNamedParameter(AIdName.class, "b://b");
  }
  @Test(expected=InjectionException.class)
  public void testInjectUnboundParsable() throws BindException, InjectionException {
    @SuppressWarnings("unchecked")
    JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder(IdentifierParser.class);
    Tang.Factory.getTang().newInjector(b.build()).getNamedInstance(IdName.class);
  }
  
}
