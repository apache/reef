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
using System.Reflection;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class NodeFactory
    {
        public static IPackageNode CreateRootPackageNode()
        {
            return new PackageNodeImpl();
        }

        public static INode CreateClassNode(INode parent, Type type)
        {
            var namedParameter = type.GetCustomAttribute<NamedParameterAttribute>();
            var isUnit = null != type.GetCustomAttribute<UnitAttribute>();
            string simpleName = type.Name;
            string fullName = type.FullName;
            bool isStatic = type.IsSealed && type.IsAbstract;
            bool injectable = true; // TODO
            bool isAssignableFromExternalConstructor = true; //TODO

            var injectableConstructors = new List<IConstructorDef>();
            var allConstructors = new List<IConstructorDef>();

            foreach (var c in type.GetConstructors())
            {
                var isConstructorInjectable = null != c.GetCustomAttribute<InjectAttribute>();

                ConstructorDefImpl constructorDef = CreateConstructorDef(injectable, c, isConstructorInjectable);

                if (isConstructorInjectable)
                {
                    injectableConstructors.Add(constructorDef);
                }
                allConstructors.Add(constructorDef);

            }

            String defaultImplementation = null;
            var defaultImpl = type.GetCustomAttribute<DefaultImplementationAttribute>();
            if (null != defaultImpl)
            {
                Type defaultImplementationClazz = defaultImpl.Value;
                defaultImplementation = defaultImplementationClazz.FullName;
            }

            return new ClassNodeImpl(parent, simpleName, fullName, isUnit, injectable, isAssignableFromExternalConstructor, injectableConstructors, allConstructors, defaultImplementation);
        }

        private static ConstructorDefImpl CreateConstructorDef(bool injectable, ConstructorInfo constructor, bool isConstructorInjectable)
        {
            var parameters = constructor.GetParameters();

            IConstructorArg[] args = new ConstructorArgImpl[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                //TODO for getInterfaceTarget() call
                Type type = parameters[i].ParameterType;
                bool isFuture = false;

                ParameterAttribute named = parameters[i].GetCustomAttribute<ParameterAttribute>();
                args[i] = new ConstructorArgImpl(type.FullName, named == null ? null : named.Value.AssemblyQualifiedName, isFuture);
            }
            return new ConstructorDefImpl(constructor.DeclaringType.FullName, args, isConstructorInjectable); 
        }

        //TimerNode, Seconds, Int32
        public static INamedParameterNode CreateNamedParameterNode(INode parent, Type type, Type argType)
        {
            Type argRawClass = ReflectionUtilities.GetRawClass(argType);

            bool isSet = false; //TODO
           
            string simpleName = type.Name;
            string fullName = type.FullName;
            string fullArgName = argType.FullName;
            string simpleArgName = argType.Name;

            NamedParameterAttribute namedParameter = type.GetCustomAttribute<NamedParameterAttribute>();

            //if (namedParameter == null)
            //{
            //    throw new IllegalStateException("Got name without named parameter post-validation!");
            //}

            string[] defaultInstanceAsStrings = new string[]{};
            string documentation = namedParameter.Documentation;
            string shortName = namedParameter.ShortName;

            return new NamedParameterNodeImpl(parent, simpleName, fullName,
                fullArgName, simpleArgName, isSet, documentation, shortName, defaultInstanceAsStrings);
        }
    }
}
