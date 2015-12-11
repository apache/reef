/**
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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.Configuration
{
    public class ConfigurationBuilderImpl : IConfigurationBuilder
    {
        public IClassHierarchy ClassHierarchy;

        public readonly IDictionary<IClassNode, IClassNode> BoundImpls = new MonotonicTreeMap<IClassNode, IClassNode>();
        public readonly IDictionary<IClassNode, IClassNode> BoundConstructors = new MonotonicTreeMap<IClassNode, IClassNode>();
        public readonly IDictionary<INamedParameterNode, String> NamedParameters = new MonotonicTreeMap<INamedParameterNode, String>();
        public readonly IDictionary<IClassNode, IConstructorDef> LegacyConstructors = new MonotonicTreeMap<IClassNode, IConstructorDef>();
        public readonly MonotonicMultiMap<INamedParameterNode, object> BoundSetEntries = new MonotonicMultiMap<INamedParameterNode, object>();
        public readonly IDictionary<INamedParameterNode, IList<object>> BoundLists = new MonotonicTreeMap<INamedParameterNode, IList<object>>();

        public readonly static string INIT = "<init>";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConfigurationBuilderImpl));

        protected ConfigurationBuilderImpl() 
        {
            this.ClassHierarchy = TangFactory.GetTang().GetDefaultClassHierarchy();
        }

        public ConfigurationBuilderImpl(IClassHierarchy classHierarchy)
        {
            this.ClassHierarchy = classHierarchy;
        }

        protected ConfigurationBuilderImpl(string[] assemblies, IConfiguration[] confs, Type[] parsers)
        {
            this.ClassHierarchy = TangFactory.GetTang().GetDefaultClassHierarchy(assemblies, parsers);
            foreach (IConfiguration tc in confs) 
            {
                AddConfiguration((ConfigurationImpl)tc);
            }
        }

        public ConfigurationBuilderImpl(ConfigurationBuilderImpl t) 
        {
            this.ClassHierarchy = t.GetClassHierarchy();
            try 
            {
                AddConfiguration(t.GetClassHierarchy(), t);
            } 
            catch (BindException e) 
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Could not copy builder", e), LOGGER); 
            }
        }

        protected ConfigurationBuilderImpl(string[] assemblies) : this(assemblies, new IConfiguration[0], new Type[0])
        {
        }

        protected ConfigurationBuilderImpl(IConfiguration[] confs) : this(new string[0], confs, new Type[0])
        {
        }

        public void AddConfiguration(IConfiguration conf)
        {
            AddConfiguration(conf.GetClassHierarchy(), ((ConfigurationImpl)conf).Builder);
        }

        private void AddConfiguration(IClassHierarchy ns, ConfigurationBuilderImpl builder)
        {
            this.ClassHierarchy = this.ClassHierarchy.Merge(ns);
            
            if (ClassHierarchy is ClassHierarchyImpl || builder.ClassHierarchy is ClassHierarchyImpl) 
            {
                if (ClassHierarchy is ClassHierarchyImpl && builder.ClassHierarchy is ClassHierarchyImpl)
                {
                    ((ClassHierarchyImpl)ClassHierarchy).Parameterparser.MergeIn(((ClassHierarchyImpl)builder.ClassHierarchy).Parameterparser);
                } 
                else 
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Attempt to merge Java and non-Java class hierarchy!  Not supported."), LOGGER);
                }
            }

            foreach (IClassNode cn in builder.BoundImpls.Keys) 
            {
                IClassNode n = null;
                builder.BoundImpls.TryGetValue(cn, out n);
                if (n != null)
                {
                    Bind(cn.GetFullName(), n.GetFullName());
                }
            }

            foreach (IClassNode cn in builder.BoundConstructors.Keys) 
            {
                IClassNode n = null;
                builder.BoundConstructors.TryGetValue(cn, out n);
                if (n != null)
                {
                    Bind(cn.GetFullName(), n.GetFullName());
                }
            }

            // The namedParameters set contains the strings that can be used to
            // instantiate new
            // named parameter instances. Create new ones where we can.
            foreach (INamedParameterNode np in builder.NamedParameters.Keys) 
            {
                string v = null;
                builder.NamedParameters.TryGetValue(np, out v);
                Bind(np.GetFullName(), v);
            }
    
            foreach (IClassNode cn in builder.LegacyConstructors.Keys) 
            {
                IConstructorDef cd = null;
                builder.LegacyConstructors.TryGetValue(cn, out cd);
                RegisterLegacyConstructor(cn, cd.GetArgs());  
            }

            foreach (KeyValuePair<INamedParameterNode, object> e in builder.BoundSetEntries) 
            {
                String name = ((INamedParameterNode)e.Key).GetFullName();
                if (e.Value is INode) 
                {
                    BindSetEntry(name, (INode)e.Value);
                } 
                else if (e.Value is string) 
                {
                    BindSetEntry(name, (string)e.Value);
                } 
                else 
                {
                    var ex = new IllegalStateException(string.Format(CultureInfo.CurrentCulture, "The value {0} set to the named parameter {1} is illegel.", e.Value, name));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }

            foreach (var p in builder.BoundLists)
            {
                BoundLists.Add(p.Key, p.Value);
            }
        }

        public IClassHierarchy GetClassHierarchy()
        {
            return this.ClassHierarchy;
        }

        public void RegisterLegacyConstructor(IClassNode cn, IList<IClassNode> args)
        {
            LegacyConstructors.Add(cn, cn.GetConstructorDef(args));
        }

        public void RegisterLegacyConstructor(string s, IList<string> args)
        {
            IClassNode cn = (IClassNode)this.ClassHierarchy.GetNode(s);
            IList<IClassNode> cnArgs = new List<IClassNode>();
            for (int i = 0; i < args.Count; i++)
            {
                cnArgs.Add((IClassNode)ClassHierarchy.GetNode(args[i]));
            }
            RegisterLegacyConstructor(cn, cnArgs);
        }

        public void RegisterLegacyConstructor(IClassNode c, IList<IConstructorArg> args)
        {
            string[] cn = new string[args.Count];

            for (int i = 0; i < args.Count; i++)
            {
                cn[i] = args[i].Gettype();
            }
            RegisterLegacyConstructor(c.GetFullName(), cn);
        }

        public IConfiguration Build()
        {
            return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
        }

        public void Bind(string key, string value)
        {
            INode n = this.ClassHierarchy.GetNode(key);
            BindNode(n, value);
        }

        private void BindNode(INode n, string value)
        {
            if (n is INamedParameterNode)
            {
                BindParameter((INamedParameterNode)n, value);
            }
            else if (n is IClassNode)
            {
                INode m = this.ClassHierarchy.GetNode(value);
                Bind((IClassNode)n, (IClassNode)m);
            }
            else
            {
                var ex = new IllegalStateException(string.Format("getNode() returned {0} which is neither a ClassNode nor a NamedParameterNode", n));
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
        }

        public void Bind(string key, string value, string aliasLanguage)
        {
            INode n = ClassHierarchy.GetNode(key, aliasLanguage);
            BindNode(n, value);
        }

        public void Bind(Types.INode key, Types.INode value)
        {
            if (key is INamedParameterNode)
            {
                BindParameter((INamedParameterNode)key, value.GetFullName());
            }
            else if (key is IClassNode)
            {
                IClassNode k = (IClassNode)key;
                if (value is IClassNode)
                {
                    IClassNode val = (IClassNode)value;
                    if (val.IsExternalConstructor() && !k.IsExternalConstructor())
                    {
                        BindConstructor(k, (IClassNode)val);
                    }
                    else
                    {
                        BindImplementation(k, (IClassNode)val);
                    }
                }
            }
        }

        public void BindParameter(INamedParameterNode name, String value)
        {
            /* Parse and discard value; this is just for type checking, skip for now*/
            if (this.ClassHierarchy is ICsClassHierarchy) 
            {
                ((ICsClassHierarchy)ClassHierarchy).Parse(name, value);
            }

            if (name.IsSet()) 
            {
                BindSetEntry((INamedParameterNode)name, value);
            } 
            else 
            {
                NamedParameters.Add(name, value);
            }
        }

        public void BindImplementation(IClassNode n, IClassNode m)
        {
            if (this.ClassHierarchy.IsImplementation(n, m))
            {
                BoundImpls.Add(n, m);
            }
            else
            {
                var ex = new ArgumentException(string.Format("Class {0} does not extend {1}.", m, n));
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
        }

        public void BindSetEntry(String iface, String impl)
        {
            BoundSetEntries.Add((INamedParameterNode)this.ClassHierarchy.GetNode(iface), impl);
        }

        public void BindSetEntry(String iface, INode impl)
        {
            BoundSetEntries.Add((INamedParameterNode)ClassHierarchy.GetNode(iface), impl);
        }

        public void BindSetEntry(INamedParameterNode iface, String impl)
        {
            BoundSetEntries.Add(iface, impl);
        }

        public void BindSetEntry(INamedParameterNode iface, INode impl)
        {
            BoundSetEntries.Add(iface, impl);
        }

        public void BindList(INamedParameterNode iface, IList<INode> impl)
        {
            IList<object> l = new List<object>();
            foreach (var n in impl)
            {
                l.Add((object)n);
            }
            BoundLists.Add(iface, l);
        }

        public void BindList(INamedParameterNode iface, IList<string> impl)
        {
            IList<object> l = new List<object>();
            foreach (var n in impl)
            {
                l.Add((object)n);
            }
            BoundLists.Add(iface, l);
        }

        public void BindList(string iface, IList<INode> impl)
        {
            BindList((INamedParameterNode)ClassHierarchy.GetNode(iface), impl);            
        }

        public void BindList(string iface, IList<string> impl)
        {
            BindList((INamedParameterNode)ClassHierarchy.GetNode(iface), impl);
        }

        public void BindConstructor(Types.IClassNode k, Types.IClassNode v)
        {
            BoundConstructors.Add(k, v);
        }

        public string ClassPrettyDefaultString(string longName)
        {
            INamedParameterNode param = (INamedParameterNode)this.ClassHierarchy.GetNode(longName);
            return param.GetSimpleArgName() + "=" + Join(",", param.GetDefaultInstanceAsStrings());
        }

        private String Join(string sep, string[] s)
        {
            if (s.Length == 0)
            {
                return null;
            }
            else
            {
                StringBuilder sb = new StringBuilder(s[0]);
                for (int i = 1; i < s.Length; i++)
                {
                    sb.Append(sep);
                    sb.Append(s[i]);
                }
                return sb.ToString();
            }
        }

        public string ClassPrettyDescriptionString(string fullName)
        {
            INamedParameterNode param = (INamedParameterNode)this.ClassHierarchy.GetNode(fullName);
            return param.GetDocumentation() + "\n" + param.GetFullName();
        }
    }
}
