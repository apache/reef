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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class ConfigurationBuilderImpl : IConfigurationBuilder
    {
        public IClassHierarchy ClassHierarchy;

        public readonly IDictionary<IClassNode, IClassNode> BoundImpls = new MonotonicTreeMap<IClassNode, IClassNode>();
        public readonly IDictionary<IClassNode, IClassNode> BoundConstructors = new MonotonicTreeMap<IClassNode, IClassNode>();
        public readonly IDictionary<INamedParameterNode, String> NamedParameters = new MonotonicTreeMap<INamedParameterNode, String>();
        public readonly IDictionary<IClassNode, IConstructorDef> LegacyConstructors = new MonotonicTreeMap<IClassNode, IConstructorDef>();

        protected ConfigurationBuilderImpl() 
        {
            this.ClassHierarchy = TangFactory.GetTang().GetDefaultClassHierarchy();
        }

        protected ConfigurationBuilderImpl(string[] assemblies, IConfiguration[] confs, Type[] parsers)
        {
            this.ClassHierarchy = TangFactory.GetTang().GetDefaultClassHierarchy(assemblies, parsers);
            foreach (IConfiguration tc in confs) 
            {
                AddConfiguration(((ConfigurationImpl) tc));
            }
        }

        public ConfigurationBuilderImpl(ConfigurationBuilderImpl t) 
        {
            this.ClassHierarchy = t.GetClassHierarchy();
            try {
                AddConfiguration(t.GetClassHierarchy(), t);
            } 
            catch (BindException e) 
            {
                throw new IllegalStateException("Could not copy builder", e);
            }
        }

        public ConfigurationBuilderImpl(IClassHierarchy classHierarchy)
        {
            this.ClassHierarchy = classHierarchy;
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
            
            //TODO
            //((ClassHierarchyImpl) ClassHierarchy).parameterParser
            //    .mergeIn(((ClassHierarchyImpl) namespace).parameterParser);

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
                //RegisterLegacyConstructor(cn, cd.GetArgs());  TODO
            }

            //for (Entry<NamedParameterNode<Set<?>>, Object> e: builder.boundSetEntries) {
            //  String name = ((NamedParameterNode<Set<T>>)(NamedParameterNode<?>)e.getKey()).getFullName();
            //  if(e.getValue() instanceof Node) {
            //    bindSetEntry(name, (Node)e.getValue());
            //  } else if(e.getValue() instanceof String) {
            //    bindSetEntry(name, (String)e.getValue());
            //  } else {
            //    throw new IllegalStateException();
            //  }
            //}
        }

        public IClassHierarchy GetClassHierarchy()
        {
            return this.ClassHierarchy;
        }

        public IConfiguration Build()
        {
            return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
//            return new ConfigurationImpl(this);
        }

        public void Bind(string key, string value)
        {
            INode n = this.ClassHierarchy.GetNode(key);
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
                throw new IllegalStateException(string.Format("getNode() returned {0} which is neither a ClassNode nor a NamedParameterNode", n));
            }
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
                        //BindConstructor(k, (IClassNode) val);
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
            //if(classHierarchy is ICsClassHierarchy) 
            //{
            //    ((ICsClassHierarchy)classHierarchy).Parse(name, value);
            //}

            //if(name.IsSet()) 
            //{
            //    BindSetEntry((INamedParameterNode)name, value);
            //} 
            //else 
            //{
                NamedParameters.Add(name, value);
            //}
        }

        public void BindImplementation(IClassNode n, IClassNode m)
        {
            if (this.ClassHierarchy.IsImplementation(n, m))
            {
                BoundImpls.Add(n, m);
            }
            else
            {
                throw new ArgumentException(string.Format("Class {0} does not extend {1}.", m, n));
            }
        }

        public void BindConstructor(Types.IClassNode k, Types.IClassNode v)
        {
            BoundConstructors.Add(k, v);
        }

        public string ClassPrettyDefaultString(string longName)
        {
            INamedParameterNode param = (INamedParameterNode) this.ClassHierarchy.GetNode(longName);
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
            INamedParameterNode param = (INamedParameterNode) this.ClassHierarchy.GetNode(fullName);
            return param.GetDocumentation() + "\n" + param.GetFullName();
        }
    }
}
