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

namespace Com.Microsoft.Tang.Implementations
{
    public class CsConfigurationBuilderImpl : ConfigurationBuilderImpl, ICsConfigurationBuilder
    {
        public CsConfigurationBuilderImpl(string[] assemblies, IConfiguration[] confs, Type[] parsers) : base(assemblies,confs,parsers)
        {
        }

        public CsConfigurationBuilderImpl(IConfiguration[] confs) : base(confs)
        {
        }

        public CsConfigurationBuilderImpl(CsConfigurationBuilderImpl impl) : base(impl)
        {
        }
        public CsConfigurationBuilderImpl(ICsClassHierarchy classHierarchy)
            : base(classHierarchy)
        {
        }
        

        public CsConfigurationBuilderImpl(string[] assemblies)
            : base(assemblies)
        {
        }

        public CsConfigurationImpl build()
        {
            return new CsConfigurationImpl(new CsConfigurationBuilderImpl(this));
        }  
        
        private INode GetNode(Type c) 
        {
            return ((ICsClassHierarchy)ClassHierarchy).GetNode(c);
        }


        public void Bind(Type iface, Type impl)
        {
            Bind(GetNode(iface), GetNode(impl));
        }

        public void BindImplementation(Type iface, Type impl)
        {
            INode cn = GetNode(iface);
            INode dn = GetNode(impl);
            if (!(cn is IClassNode)) 
            {
                throw new BindException(
                    "bindImplementation passed interface that resolved to " + cn
                    + " expected a ClassNode<?>");
            }
            if (!(dn is IClassNode)) 
            {
                throw new BindException(
                    "bindImplementation passed implementation that resolved to " + dn
                    + " expected a ClassNode<?>");
            }
            BindImplementation((IClassNode) cn, (IClassNode) dn);
 
        }

        public void BindNamedParameter(Type name, string value)
        {
            INode np = GetNode(name);
            if (np is INamedParameterNode) 
            {
                BindParameter((INamedParameterNode)np, value);
            } 
            else 
            {
                throw new BindException(
                    "Detected type mismatch when setting named parameter " + name
                    + "  Expected NamedParameterNode, but namespace contains a " + np);
            }
        }

        public void BindNamedParameter(Type iface, Type impl)
        {
            INode ifaceN = GetNode(iface);
            INode implN = GetNode(impl);
            if (!(ifaceN is INamedParameterNode)) 
            {
                throw new BindException("Type mismatch when setting named parameter " + ifaceN
                    + " Expected NamedParameterNode");
            }
            Bind(ifaceN, implN);
        }
    }
}
