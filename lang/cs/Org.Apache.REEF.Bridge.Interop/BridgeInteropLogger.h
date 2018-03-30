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

#ifndef BRIDGE_INTEROP_LOGGER_H
#define BRIDGE_INTEROP_LOGGER_H

#include <cstdint>
#include <string>

namespace Org
{
    namespace Apache
    {
        namespace REEF
        {
            namespace Driver
            {
                namespace Bridge
                {
                    ///
                    /// The BridgeInteropLogger provids the C++ representation of the 
                    /// the C# bridge logger.
                    ///
                    class BridgeInteropLogger
                    {
                    public:
                        BridgeInteropLogger(std::wstring classname);
                        ~BridgeInteropLogger();
                        
                        void Log(std::wstring const& message);
                        void LogStart(std::wstring const& message);
                        void LogStop(std::wstring const& message);
                        void LogError(std::wstring const& message, std::wstring const& excep);

                    private:
                        std::int32_t _index;
                    };
                }
            }

        }
    }
}
#endif // BRIDGE_INTEROP_LOGGER_H

