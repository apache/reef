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

namespace Com.Microsoft.Tang.Util
{
    public class ReflectionUtilities
    {
        public static String GetFullName(Type name)
        {
            return GetRawClass(name).Name;
        }

        public static Type GetRawClass(Type type)
        {
            return type;
            //if (type.IsClass)
            //{
            //    return type;
            //}
            //else if (type.IsGenericType)
            //{
            //    return type.GetGenericTypeDefinition();
            //}
            //else if (type.IsGenericParameter)
            //{
            //    return typeof(object);
            //}
            //else
            //{
            //    throw new ArgumentException("Can't getRawClass for " + type + " of unknown type " + type);
            //}
        }

        public static Type getInterfaceTarget(Type iface, Type type)
        {
            //TODO
            return type;
        }

          public static IEnumerable<Type> ClassAndAncestors(Type c) 
          {
              List<Type> workQueue = new List<Type>();
              workQueue.Add(c);
              //TODO
              return workQueue;
          }

        public static Type BoxClass(Type c) 
        {
            if (c.IsPrimitive && c != typeof(Type)) 
            {
            if (c == typeof(bool)) {
                return typeof(Boolean);
            } else if (c == typeof(byte)) {
                return typeof(Byte);
            } else if (c == typeof(char)) {
                return typeof(Char);
            } else if (c == typeof(short)) {
                return typeof(Int16);
            } else if (c == typeof(int)) {
                return typeof(Int32);
            }
            else if (c == typeof(long))
            {
                return typeof(long);
            } else if (c == typeof(float)) {
                return typeof(Single);
            } else if (c == typeof(double)) {
                return typeof(Double);
            } else {
                throw new NotSupportedException(
                    "Encountered unknown primitive type!");
            }
            } else {
                return c;
            }
  }
    }
}
