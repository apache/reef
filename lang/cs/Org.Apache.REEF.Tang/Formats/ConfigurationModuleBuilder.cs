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
using System.Reflection;
using System.Text;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Formats
{
    public class ConfigurationModuleBuilder
    {
        public readonly ICsConfigurationBuilder B = TangFactory.GetTang().NewConfigurationBuilder();
        public readonly MonotonicHashSet<FieldInfo> ReqDecl = new MonotonicHashSet<FieldInfo>();
        public readonly MonotonicHashSet<FieldInfo> OptDecl = new MonotonicHashSet<FieldInfo>();
        public readonly MonotonicHashSet<object> SetOpts = new MonotonicHashSet<object>();
        public readonly MonotonicHashMap<object, FieldInfo> Map = new MonotonicHashMap<object, FieldInfo>();
        public readonly MonotonicHashMap<Type, object> FreeImpls = new MonotonicHashMap<Type, object>();
        public readonly MonotonicHashMap<Type, object> FreeParams = new MonotonicHashMap<Type, object>(); // Type must extends from Name<>

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConfigurationModuleBuilder));
        private static readonly ISet<Type> ParamBlacklist = new MonotonicHashSet<Type>(new Type[] { typeof(IParam<>), typeof(IImpl<>) });
        private static readonly ISet<string> ParamTypes =
            new MonotonicHashSet<string>(new string[] { typeof(RequiredImpl<>).Name, typeof(OptionalImpl<>).Name, typeof(RequiredParameter<>).Name, typeof(OptionalParameter<>).Name });

        private readonly MonotonicHashSet<FieldInfo> reqUsed = new MonotonicHashSet<FieldInfo>();
        private readonly MonotonicHashSet<FieldInfo> optUsed = new MonotonicHashSet<FieldInfo>();
        private readonly MonotonicHashMap<Type, string> lateBindClazz = new MonotonicHashMap<Type, string>();

        public ConfigurationModuleBuilder()
        {
            foreach (FieldInfo f in GetType().GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            {
                Type t = f.FieldType;
                if (ParamBlacklist.Contains(t)) 
                {
                    var e = new ClassHierarchyException(
                    "Found a field of type " + t + " which should be a Required/Optional Parameter/Implementation instead");
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                } 
                if (ParamTypes.Contains(t.Name)) 
                {
                    if (!f.IsPublic) 
                    {
                        var e = new ClassHierarchyException("Found a non-public configuration option in " + GetType() + ": " + f);
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                
                    if (!f.IsStatic) 
                    {
                        var e = new ClassHierarchyException("Found a non-static configuration option in " + GetType() + ": " + f);
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    if (!f.IsInitOnly)
                    {
                        var e = new ClassHierarchyException("Found a non-readonly configuration option in " + GetType() + ": " + f);
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    object o = null;
                    try 
                    {
                        o = f.GetValue(null);
                    } 
                    catch (ArgumentException e)  
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        var ex = new ClassHierarchyException("Could not look up field instance in " + GetType() + " field: " + f, e);
                        Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    catch (FieldAccessException e) 
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        var ex = new ClassHierarchyException("Could not look up field instance in " + GetType() + " field: " + f, e);
                        Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }

                    if (Map.ContainsKey(o)) 
                    {
                        FieldInfo fi;
                        Map.TryGetValue(o, out fi);
                        var e = new ClassHierarchyException("Detected aliased instances in class " + GetType() + " for fields " + fi + " and " + f);
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    if (ReflectionUtilities.IsGenericTypeof(typeof(RequiredImpl<>), t) || ReflectionUtilities.IsGenericTypeof(typeof(RequiredParameter<>), t))  
                    {
                        ReqDecl.Add(f);
                    } 
                    else 
                    {
                        OptDecl.Add(f);
                    }
                    Map.Add(o, f);
               }
            }
        }

        private ConfigurationModuleBuilder(ConfigurationModuleBuilder c)
        {
            try
            {
                B.AddConfiguration(c.B.Build());
            }
            catch (BindException e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                var ex = new ClassHierarchyException("Build error in ConfigurationModuleBuilder: " + e);
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            ReqDecl.UnionWith(c.ReqDecl);
            OptDecl.UnionWith(c.OptDecl);
            reqUsed.UnionWith(c.reqUsed);
            optUsed.UnionWith(c.optUsed);
            SetOpts.UnionWith(c.SetOpts);
            Map.AddAll(c.Map);
            FreeImpls.AddAll(c.FreeImpls);
            FreeParams.AddAll(c.FreeParams);
            lateBindClazz.AddAll(c.lateBindClazz);
        }

        public ConfigurationModuleBuilder Merge(ConfigurationModule d) 
        {
            if (d == null) 
            {
                var e = new NullReferenceException("If merge() was passed a static final field that is initialized to non-null, then this is almost certainly caused by a circular class dependency.");
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            try 
            {
                d.AssertStaticClean();
            } 
            catch (ClassHierarchyException ex) 
            {
                Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new ClassHierarchyException(ReflectionUtilities.GetAssemblyQualifiedName(GetType()) + ": detected attempt to merge with ConfigurationModule that has had set() called on it", ex);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            ConfigurationModuleBuilder c = DeepCopy();
            try 
            {
                c.B.AddConfiguration(d.Builder.B.Build());
            } 
            catch (BindException ex) 
            {
                Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new ClassHierarchyException("Error in AddConfiguration in Merge: " + ex);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            c.ReqDecl.AddAll(d.Builder.ReqDecl);
            c.OptDecl.AddAll(d.Builder.OptDecl);
            c.reqUsed.AddAll(d.Builder.reqUsed);
            c.optUsed.AddAll(d.Builder.optUsed);
            c.SetOpts.AddAll(d.Builder.SetOpts);
            //// c.ListOpts.AddAll(d.Builder.ListOpts);
            c.Map.AddAll(d.Builder.Map);
            c.FreeImpls.AddAll(d.Builder.FreeImpls);
            c.FreeParams.AddAll(d.Builder.FreeParams);
            c.lateBindClazz.AddAll(d.Builder.lateBindClazz);
    
            return c;
        }

        public ConfigurationModuleBuilder BindSetEntry<U, T>(GenericType<U> iface, string impl) 
            where U : Name<ISet<T>>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try 
            {
                ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.B;
                b.BindSetEntry(typeof(U), impl);
            } 
            catch (BindException ex) 
            {
                Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new ClassHierarchyException("Error in BindSetEntry: " + ex);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindSetEntry<U, V, T>(GenericType<U> iface, GenericType<V> impl) 
            where U : Name<ISet<T>> 
            where V : T
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try 
            {
                c.B.BindSetEntry<U, V, T>(iface, impl);
            } 
            catch (BindException ex) 
            {
                Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new ClassHierarchyException("Error in BindSetEntry: " + ex);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindSetEntry<U, T>(GenericType<U> iface, IImpl<T> opt) 
            where U : Name<ISet<T>> 
        {
            ConfigurationModuleBuilder c = DeepCopy();
            Type ifaceType = typeof(U);

            c.ProcessUse(opt);
            c.FreeImpls.Add(ifaceType, opt);  
            
            if (!SetOpts.Contains(opt)) 
            { 
                c.SetOpts.Add(opt); 
            }
            return c;
        }

        public ConfigurationModuleBuilder BindSetEntry<U, T>(GenericType<U> iface, IParam<T> opt)
            where U : Name<ISet<T>>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            Type ifaceType = typeof(U);
            c.ProcessUse(opt);

            c.FreeParams.Add(ifaceType, opt);
            if (!SetOpts.Contains(opt)) 
            { 
                c.SetOpts.Add(opt); 
            }
            return c;
        }

        public ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IList<string> impl)
            where U : Name<IList<T>>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try
            {
                ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.B;
                b.BindList(typeof(U), impl);
            }
            catch (BindException ex)
            {
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(new ClassHierarchyException("Error in BindList: " + ex), Level.Error, LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IImpl<IList<T>> opt)
            where U : Name<IList<T>>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            Type ifaceType = typeof(U);

            c.ProcessUse(opt);
            c.FreeImpls.Add(ifaceType, opt);
            return c;
        }

        public ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IParam<IList<T>> opt)
            where U : Name<IList<T>>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            Type ifaceType = typeof(U);
            c.ProcessUse(opt);

            c.FreeParams.Add(ifaceType, opt);
            return c;
        }

        public ConfigurationModuleBuilder BindImplementation<U, T>(GenericType<U> iface, GenericType<T> impl) 
            where T : U
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try 
            {
                c.B.BindImplementation(iface, impl);
            } 
            catch (BindException e) 
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException("Error in BindImplementation: ", e), LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindImplementation<T>(GenericType<T> iface, string impl) 
        {
            ConfigurationModuleBuilder c = DeepCopy();

            c.lateBindClazz.Add(typeof(T), impl);
            return c;
        }

        public ConfigurationModuleBuilder BindImplementation<U, T>(GenericType<T> iface, IImpl<U> opt) 
            where U : T
        {
            ConfigurationModuleBuilder c = DeepCopy();
            c.ProcessUse(opt);
            Type ifaceType = typeof(T);
            c.FreeImpls.Add(ifaceType, opt);
            return c;
        }

        public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> name, string value) 
            where U : Name<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try 
            {
                c.B.BindNamedParameter<U, T>(name, value);
            } 
            catch (BindException e) 
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException("Error in BindNamedParameter: ", e), LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> name, IParam<T> opt)
            where U : Name<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            c.ProcessUse(opt);
            Type nameType = typeof(U);
            c.FreeParams.Add(nameType, opt);
            return c;
        }

        // public final <T> ConfigurationModuleBuilder bindNamedParameter(Class<? extends Name<T>> iface, Class<? extends T> impl)  
        // if V is T, you'd better to use public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> iface, GenericType<T> impl) defined below
        public ConfigurationModuleBuilder BindNamedParameter<U, V, T>(GenericType<U> iface, GenericType<V> impl)
            where U : Name<T>
            where V : T
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try
            {
                c.B.BindNamedParameter<U, V, T>(iface, impl);
            }
            catch (BindException e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException("Error in BindNamedParameter: ", e), LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> iface, GenericType<T> impl)
            where U : Name<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try
            {
                c.B.BindNamedParameter<U, T, T>(iface, impl);
            }
            catch (BindException e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException("Error in BindNamedParameter: ", e), LOGGER);
            }
            return c;
        }

        // public final <T> ConfigurationModuleBuilder bindNamedParameter(Class<? extends Name<T>> iface, Impl<? extends T> opt)
        // if ValueType is T, you would better to use public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> iface, IImpl<T> opt)
        public ConfigurationModuleBuilder BindNamedParameter<U, V, T>(GenericType<U> iface, IImpl<V> opt)
            where U : Name<T>
            where V : T
        {
            ConfigurationModuleBuilder c = DeepCopy();
            c.ProcessUse(opt);
            Type ifaceType = typeof(U);
            c.FreeImpls.Add(ifaceType, opt);
            return c;
        }

        public ConfigurationModuleBuilder BindNamedParameter<U, T>(GenericType<U> iface, IImpl<T> opt)
            where U : Name<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            c.ProcessUse(opt);
            Type ifaceType = typeof(U);
            c.FreeImpls.Add(ifaceType, opt);
            return c;
        }

        public ConfigurationModuleBuilder BindConstructor<T, U>(GenericType<T> clazz, GenericType<U> constructor)
            where U : IExternalConstructor<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            try
            {
               c.B.BindConstructor<T, U>(clazz, constructor);
            }
            catch (BindException e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException("Error in BindConstructor: ", e), LOGGER);
            }
            return c;
        }

        public ConfigurationModuleBuilder BindConstructor<T, U>(GenericType<T> cons, IImpl<U> v)
            where U : IExternalConstructor<T>
        {
            ConfigurationModuleBuilder c = DeepCopy();
            c.ProcessUse(v);
            Type consType = typeof(T);
            var i = (IImpl<object>)v;
            c.FreeImpls.Add(consType, i);
            return c;
        }

        public ConfigurationModule Build()
        {
            ConfigurationModuleBuilder c = DeepCopy();
    
            if (!(c.reqUsed.ContainsAll(c.ReqDecl) && c.optUsed.ContainsAll(c.OptDecl))) 
            {
                ISet<FieldInfo> fset = new MonotonicHashSet<FieldInfo>();
                foreach (FieldInfo f in c.ReqDecl) 
                {
                    if (!c.reqUsed.Contains(f)) 
                    {
                        fset.Add(f);
                    }
                }
                foreach (FieldInfo f in c.OptDecl) 
                {
                    if (!c.optUsed.Contains(f)) 
                    {
                        fset.Add(f);
                    }
                }
                var e = new ClassHierarchyException(
                    "Found declared options that were not used in binds: "
                    + ToString(fset));
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            foreach (Type clz in c.lateBindClazz.Keys) 
            {
                try 
                {
                    c.B.Bind(ReflectionUtilities.GetAssemblyQualifiedName(clz), c.lateBindClazz.Get(clz));
                } 
                catch (NameResolutionException e) 
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    var ex = new ClassHierarchyException("ConfigurationModule refers to unknown class: " + c.lateBindClazz.Get(clz), e);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                } 
                catch (BindException e) 
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    var ex = new ClassHierarchyException("bind failed while initializing ConfigurationModuleBuilder", e);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            return new ConfigurationModule(c);
        }

        public ConfigurationModuleBuilder DeepCopy() 
        {
            return new ConfigurationModuleBuilder(this);
        }

        public string ToString(ISet<FieldInfo> s) 
        {
            StringBuilder sb = new StringBuilder("{");
            bool first = true;
            foreach (FieldInfo f in s) 
            {
                sb.Append((first ? " " : ", ") + f.Name);
                first = false;
            }
            sb.Append(" }");
            return sb.ToString();
        }

        private void ProcessUse(object impl)
        {
            FieldInfo f;
            Map.TryGetValue(impl, out f);
            if (f == null)
            {
                var e = new ClassHierarchyException("Unknown Impl/Param when binding " + impl.GetType().Name + ".  Did you pass in a field from some other module?");
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            if (!reqUsed.Contains(f))
            {
                reqUsed.Add(f);
            }
            if (!optUsed.Contains(f))
            {
                optUsed.Add(f);
            }
        }
    }
}