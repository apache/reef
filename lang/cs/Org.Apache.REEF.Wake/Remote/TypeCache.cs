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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Cache used to store the constructor functions to instantiate various Types.
    /// It is assumed that all types are inherited from the base type T
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TypeCache<T>
    {
        private const BindingFlags ConstructorFlags =
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        /// <summary>
        /// Cache that stores the constructors for already used types using the assmebly name
        /// </summary>
        private readonly Dictionary<string, Func<T>> _typeConstructorMapping = new Dictionary<string, Func<T>>();

        public T GetInstance(string typeString)
        {
            if (!_typeConstructorMapping.ContainsKey(typeString))
            {
                var type = Type.GetType(typeString);

                if (type != null)
                {
                    _typeConstructorMapping[typeString] = GetActivator(type);
                }
            }

            return _typeConstructorMapping[typeString]();
        }

        /// <summary>
        /// Returns the constructor for type T given actual type. Type can be
        /// that of inherited class.
        /// <param name="actualType">The actual type for which we want to create the constructor.</param>
        /// <returns>The constructor function</returns>
        /// </summary>
        private Func<T> GetActivator(Type actualType)
        {
            ConstructorInfo constructor;
            if (actualType.IsValueType)
            {
                // For struct types, there is an implicit default constructor.
                constructor = null;
            }
            else if (!TryGetDefaultConstructor(actualType, out constructor))
            {
                throw new Exception("could not get default constructor");
            }
            NewExpression nex = constructor == null ? Expression.New(actualType) : Expression.New(constructor);
            var body = Expression.Convert(nex, typeof (T));
            Expression<Func<T>> lambda = Expression.Lambda<Func<T>>(body);

            return lambda.Compile();
        }

        /// <summary>
        /// Fills the constructor information and meta-data
        /// </summary>
        /// <param name="type">The type for which constructor needs to be created</param>
        /// <param name="constructor">The information and meta data for the constructor creation</param>
        /// <returns></returns>
        private bool TryGetDefaultConstructor(Type type, out ConstructorInfo constructor)
        {
            // first, determine if there is a suitable constructor
            if (type.IsAbstract || type.IsInterface)
            {
                constructor = null;
                return false;
            }

            constructor = type.GetConstructor(ConstructorFlags, null, Type.EmptyTypes, null);
            return null != constructor;
        }
    }
}
