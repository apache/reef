// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Format
{
    public interface IFoo
    {
        int getFooness();
    }

    interface ISuper
    {
    }

    public class TestConfigurationModule
    {
        [Fact]
        public void SmokeTest() 
        {
            // Here we set some configuration values.  In true tang style,
            // you won't be able to set them more than once ConfigurationModule's
            // implementation is complete.
            Type fooType = typeof(Org.Apache.REEF.Tang.Tests.Format.IFoo);
            
            IConfiguration c = MyConfigurationModule.Conf
                .Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class)
                .Set(MyConfigurationModule.FooNess, "12")
                .Build();
                IFoo f = (IFoo)TangFactory.GetTang().NewInjector(c).GetInstance(fooType);
                Assert.Equal(f.getFooness(), 12);
          }

        [Fact]
        public void SmokeTestConfig()
        {
            // Here we set some configuration values.  In true tang style,
            // you won't be able to set them more than once ConfigurationModule's
            // implementation is complete.
            Type fooType = typeof(Org.Apache.REEF.Tang.Tests.Format.IFoo);

            IConfiguration c = MyConfigurationModule.Conf
                .Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class)
                .Set(MyConfigurationModule.FooNess, "12")
                .Build();
            IFoo f = (IFoo)TangFactory.GetTang().NewInjector(c).GetInstance(fooType);
            Assert.Equal(f.getFooness(), 12);
            AvroConfigurationSerializer serializerCs = new AvroConfigurationSerializer();

            serializerCs.ToFileStream(c, "TangTestCs.avroconf");
            var c3 = serializerCs.FromFileStream("TangTestCs.avroconf");
            IFoo f3 = (IFoo)TangFactory.GetTang().NewInjector(c3).GetInstance(fooType);
            Assert.Equal(f3.getFooness(), 12);

            serializerCs.ToFile(c, "TangTestCs1.avro");
            var c4 = serializerCs.FromFile("TangTestCs1.avro");
            IFoo f4 = (IFoo)TangFactory.GetTang().NewInjector(c4).GetInstance(fooType);
            Assert.Equal(f4.getFooness(), 12);

            IConfigurationSerializer serializerImpl = (IConfigurationSerializer)TangFactory.GetTang().NewInjector().GetInstance(typeof(IConfigurationSerializer));
            serializerImpl.ToFile(c, "TangTestCs1.avro");
            var c5 = serializerImpl.FromFile("TangTestCs1.avro");
            IFoo f5 = (IFoo)TangFactory.GetTang().NewInjector(c5).GetInstance(fooType);
            Assert.Equal(f5.getFooness(), 12);
        }

        [Fact]
        public void OmitOptionalTest()  
        {
            Type fooType = typeof(Org.Apache.REEF.Tang.Tests.Format.IFoo);

            IConfiguration c = MyConfigurationModule.Conf
                .Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class)
                .Build();
            IFoo f = (IFoo)TangFactory.GetTang().NewInjector(c).GetInstance(fooType);
            Assert.Equal(f.getFooness(), 42);
        }

        [Fact]
        public void OmitRequiredTest()
        {
            string msg = null;
            try 
            {
                MyConfigurationModule.Conf
                .Set(MyConfigurationModule.FooNess, "12")
                .Build();
                msg = "Attempt to build configuration before setting required option(s): { THE_FOO }";
            } 
            catch (Exception) 
            {
            }
            Assert.Null(msg);
        }

        [Fact]
        public void BadConfTest()
        {
            string msg = null;
            try
            {
                object obj = MyMissingBindConfigurationModule.BadConf;
                msg = "Found declared options that were not used in binds: { FOO_NESS }" + obj;
            }
            catch (Exception)
            {
            }
            Assert.Null(msg);
        }

        [Fact]
        public void NonExistentStringBindOK()
        {
            new MyBadConfigurationModule().BindImplementation(GenericType<IFoo>.Class, "i.do.not.exist");
        }

        [Fact]
        public void NonExistentStringBindNotOK()
        {
            string msg = null;
            try
            {
                new MyBadConfigurationModule().BindImplementation(GenericType<IFoo>.Class, "i.do.not.exist").Build();
                msg = "ConfigurationModule refers to unknown class: i.do.not.exist";
            }
            catch (Exception)
            {
            }
            Assert.Null(msg);            
        }

        [Fact]
        public void MultiBindTest() 
        {
            // Here we set some configuration values.  In true tang style,
            // you won't be able to set them more than once ConfigurationModule's
            // implementation is complete.
            IConfiguration c = MultiBindConfigurationModule.Conf
                .Set(MultiBindConfigurationModule.TheFoo, GenericType<FooImpl>.Class)
                .Set(MultiBindConfigurationModule.FOONESS, "12")
                .Build();
            IFoo f = (IFoo)TangFactory.GetTang().NewInjector(c).GetInstance(typeof(IFoo));
            IFoo g = (IFoo)TangFactory.GetTang().NewInjector(c).GetInstance(typeof(object));
            Assert.Equal(f.getFooness(), 12);
            Assert.Equal(g.getFooness(), 12);
            Assert.False(f == g);
          }

        [Fact]
        public void ForeignSetTest()
        {
            string msg = null;
            try
            {
                MultiBindConfigurationModule.Conf.Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class);
                msg = "Unknown Impl/Param when setting RequiredImpl.  Did you pass in a field from some other module?";
            }
            catch (Exception)
            {
            }
            Assert.Null(msg);
        }

        [Fact]
        public void ForeignBindTest()
        {
            string msg = null;
            try
            {
                new MyConfigurationModule().BindImplementation(GenericType<object>.Class, MultiBindConfigurationModule.TheFoo);
                msg = "Unknown Impl/Param when binding RequiredImpl.  Did you pass in a field from some other module?";
            }
            catch (Exception)
            {
            }
            Assert.Null(msg);
        }

        [Fact]
        public void SingletonTest() 
        {
            IConfiguration c = new MyConfigurationModule()
              .BindImplementation(GenericType<IFoo>.Class, MyConfigurationModule.TheFoo)
              .BindNamedParameter(GenericType<Fooness>.Class, MyConfigurationModule.FooNess)
              .Build()
              .Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class)
              .Build();
            IInjector i = TangFactory.GetTang().NewInjector(c);
            Assert.True(i.GetInstance(typeof(IFoo)) == i.GetInstance(typeof(IFoo)));
        }

        [Fact]
        public void ImmutablilityTest() 
        {
            // builder methods return copies; the original module is immutable
            ConfigurationModule builder1 = MyConfigurationModule.Conf
            .Set(MyConfigurationModule.TheFoo, GenericType<FooImpl>.Class);
   
            Assert.False(builder1 == MyConfigurationModule.Conf);

            IConfiguration config1 = builder1.Build();
  
            // reusable
            IConfiguration config2 = MyConfigurationModule.Conf
            .Set(MyConfigurationModule.TheFoo, GenericType<FooAltImpl>.Class)
            .Build();

            // instantiation of each just to be sure everything is fine in this situation
            IInjector i1 = TangFactory.GetTang().NewInjector(config1);
            IInjector i2 = TangFactory.GetTang().NewInjector(config2);
            Assert.Equal(42, ((IFoo)i1.GetInstance(typeof(IFoo))).getFooness());
            Assert.Equal(7, ((IFoo)i2.GetInstance(typeof(IFoo))).getFooness());
        }

        [Fact]
        public void SetParamTest() 
        {
            IConfiguration c = SetConfigurationModule.CONF
                .Set(SetConfigurationModule.P, "a")
                .Set(SetConfigurationModule.P, "b")
                .Build();
    
            ISet<string> s = (ISet<string>)TangFactory.GetTang().NewInjector(c).GetNamedInstance(typeof(SetName));
            Assert.Equal(s.Count, 2);
            Assert.True(s.Contains("a"));
            Assert.True(s.Contains("b"));
        }

        [Fact]
        public void SetClassTest() 
        {
            IConfiguration c = SetClassConfigurationModule.CONF
                .Set(SetClassConfigurationModule.P, GenericType<SubA>.Class)
                .Set(SetClassConfigurationModule.P, GenericType<SubB>.Class)
                .Build();
            ISet<ISuper> s = (ISet<ISuper>)TangFactory.GetTang().NewInjector(c).GetNamedInstance(typeof(SetClass));
            Assert.Equal(2, s.Count);
            
            bool sawA = false, sawB = false;    
            foreach (ISuper sup in s)
            {
                if (sup is SubA) 
                {
                    sawA = true;
                } 
                else if (sup is SubB) 
                {
                    sawB = true;
                } 
                else 
                {
                    Assert.True(false);
                }
            }
            Assert.True(sawA && sawB);
        }

        [Fact]
        public void SetClassRoundTripTest() 
        {
            IConfiguration c = SetClassConfigurationModule.CONF
                .Set(SetClassConfigurationModule.P, GenericType<SubA>.Class)
                .Set(SetClassConfigurationModule.P, GenericType<SubB>.Class)
                .Build();
            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(c);

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            IConfiguration c2 = serializer.FromString(serializer.ToString(cb.Build()));

            // ConfigurationFile.AddConfiguration(cb, ConfigurationFile.ToConfigurationString(c));
            ISet<ISuper> s = (ISet<ISuper>)TangFactory.GetTang().NewInjector(c2).GetNamedInstance(typeof(SetClass));
            Assert.Equal(2, s.Count);
            bool sawA = false, sawB = false;
            foreach (ISuper sup in s)
            {
                if (sup is SubA)
                {
                    sawA = true;
                }
                else if (sup is SubB)
                {
                    sawB = true;
                }
                else
                {
                    Assert.True(false);
                }
            }
            Assert.True(sawA && sawB);
        }

        [Fact]
        public void ErrorOnStaticTimeSet()
        {
            string msg = null;
            try
            {
                StaticTimeSet.CONF.AssertStaticClean();
                msg =
                    " Detected statically set ConfigurationModule Parameter / Implementation.  set() should only be used dynamically.  Use bind...() instead.";
            }
            catch (ClassHierarchyException)
            {
            }
            Assert.Null(msg);
        }

         [Fact]
         public void ErrorOnSetMerge()
         {
             ConfigurationModuleBuilder cb = null;
             try
             {
                 ConfigurationModuleBuilder b = new ConfigurationModuleBuilder();
                 cb = b.Merge(StaticTimeSet.CONF);
             }
             catch (ClassHierarchyException e)
             {
                 System.Diagnostics.Debug.WriteLine(e);
             }
             Assert.Null(cb);
        }
    }

    [NamedParameter("Fooness", "Fooness", "42")]
    public class Fooness : Name<int>
    {        
    }

    public class FooImpl : IFoo
    {
        private readonly int fooness;

        [Inject]
        FooImpl([Parameter(typeof(Fooness))] int fooness) 
        { 
            this.fooness = fooness; 
        }

        public int getFooness()
        {
            return this.fooness;
        }
    }
    
    public class FooAltImpl : IFoo 
    {
        private readonly int fooness;
        
        [Inject]
        FooAltImpl([Parameter(Value = typeof(Fooness))] int fooness)
        {
            this.fooness = fooness;
        }
    
        public int getFooness() 
        {
            return 7;
        }
    }

    public sealed class MyConfigurationModule : ConfigurationModuleBuilder 
    {    
      // Tell us what implementation you want, or else!!    
      [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
      public static readonly RequiredImpl<IFoo> TheFoo = new RequiredImpl<IFoo>();

      // If you want, you can change the fooness.
      [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
      public static readonly OptionalParameter<int> FooNess = new OptionalParameter<int>();

      // This binds the above to tang configuration stuff.  You can use parameters more than
      // once, but you'd better use them all at least once, or I'll throw exceptions at you.
      [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
      public static readonly ConfigurationModule Conf = new MyConfigurationModule()
        .BindImplementation(GenericType<IFoo>.Class, MyConfigurationModule.TheFoo)
        .BindNamedParameter(GenericType<Fooness>.Class, MyConfigurationModule.FooNess)
        .Build();
    }

    public class MyMissingBindConfigurationModule : ConfigurationModuleBuilder 
    {    
      // Tell us what implementation you want, or else!!    
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly RequiredImpl<IFoo> TheFoo = new RequiredImpl<IFoo>();
  
        // If you want, you can change the fooness.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly OptionalParameter<int> FooNess = new OptionalParameter<int>();
  
        // This conf doesn't use FOO_NESS.  Expect trouble below
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly ConfigurationModule BadConf = new MyMissingBindConfigurationModule()
            .BindImplementation(GenericType<IFoo>.Class, MyMissingBindConfigurationModule.TheFoo)
            .Build();
    }

    public class MyBadConfigurationModule : ConfigurationModuleBuilder 
    {    
    }

    public class MultiBindConfigurationModule : ConfigurationModuleBuilder 
    {    
        // Tell us what implementation you want, or else!!    
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly RequiredImpl<IFoo> TheFoo = new RequiredImpl<IFoo>();

        // If you want, you can change the fooness.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly OptionalParameter<int> FOONESS = new OptionalParameter<int>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Required by Tang")]
        public static readonly ConfigurationModule Conf = new MultiBindConfigurationModule()
          .BindImplementation(GenericType<IFoo>.Class, MultiBindConfigurationModule.TheFoo)
          .BindImplementation(GenericType<object>.Class, MultiBindConfigurationModule.TheFoo)
          .BindNamedParameter(GenericType<Fooness>.Class, MultiBindConfigurationModule.FOONESS)
          .Build();    
    }

    [NamedParameter]
    class SetName : Name<ISet<string>> 
    {
    }

    class SetConfigurationModule : ConfigurationModuleBuilder 
    {
        public static readonly RequiredParameter<string> P = new RequiredParameter<string>();

        public static readonly ConfigurationModule CONF = new SetConfigurationModule()
            .BindSetEntry(GenericType<SetName>.Class, SetConfigurationModule.P)
            .Build();
    }

    [NamedParameter]
    class SetClass : Name<ISet<ISuper>> 
    {
    }

    class SetClassConfigurationModule : ConfigurationModuleBuilder 
    {
        public static readonly RequiredParameter<ISuper> P = new RequiredParameter<ISuper>();
        public static readonly ConfigurationModule CONF = new SetClassConfigurationModule()
        .BindSetEntry(GenericType<SetClass>.Class, SetClassConfigurationModule.P)
        .Build();
    }

    class SubA : ISuper 
    {
        [Inject]
        public SubA() 
        {
        }
    }

    class SubB : ISuper 
    {
        [Inject]
        public SubB() 
        {
        }
    }
    
    class StaticTimeSet : ConfigurationModuleBuilder 
    {
        public static readonly OptionalImpl<ISuper> X = new OptionalImpl<ISuper>();
        public static readonly ConfigurationModule CONF = new StaticTimeSet()
            .BindImplementation(GenericType<ISuper>.Class, X)
            .Build()
            .Set(X, GenericType<SubA>.Class);
    }
}