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
using System.Runtime.InteropServices;

namespace Org.Apache.REEF.Driver.Bridge.Interop
{
    public class HGlobalPtr : IDisposable
    {
        public IntPtr Addr { get; private set; }

        public HGlobalPtr(object obj)
        {
            if (Addr == null)
            {
                Addr = IntPtr.Zero;
            }
            else
            {
                Addr = Marshal.AllocHGlobal(Marshal.SizeOf(obj));
                Marshal.StructureToPtr(obj, Addr, false);
            }
        }

        ~HGlobalPtr()
        {
            if (Addr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(Addr);
                Addr = IntPtr.Zero;
            }
        }

        public void Dispose()
        {
            Marshal.FreeHGlobal(Addr);
            Addr = IntPtr.Zero;
            GC.SuppressFinalize(this);
        }

        public static implicit operator IntPtr(HGlobalPtr obj)
        {
            return obj.Addr;
        }
    }
}
