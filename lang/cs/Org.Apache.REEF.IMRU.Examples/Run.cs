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
using System.Globalization;
using Org.Apache.REEF.IMRU.OnREEF;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.IMRU.Examples.MapperCount;
using Org.Apache.REEF.IMRU.OnREEF.Client;

namespace Org.Apache.REEF.IMRU.Examples
{
    /// <summary>
    /// Runs IMRU for mapper count either in localruntime or on cluster.
    /// </summary>
    public class Run
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            int numNodes = 2;
            int startPort = 8900;
            int portRange = 1000;

            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = bool.Parse(args[0].ToLower());
                }

                if (args.Length > 1)
                {
                    numNodes = int.Parse(args[1]);
                }

                if (args.Length > 2)
                {
                    startPort = int.Parse(args[2]);
                }

                if (args.Length > 3)
                {
                    portRange = int.Parse(args[3]);
                }
            }


            var externalConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof (TcpPortRangeStart),
                    startPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof (TcpPortRangeCount),
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            MapperCount.MapperCount tested = null;

            if (!runOnYarn)
            {
                tested =
                    TangFactory.GetTang()
                        .NewInjector(new[]
                        {
                            OnREEFIMRURunTimeConfigurations<int, int, int>.GetLocalIMRUConfiguration(numNodes),
                            externalConfig
                        }).GetInstance<MapperCount.MapperCount>();
            }
            else
            {
                tested = TangFactory.GetTang()
                    .NewInjector(new[]
                    {
                        OnREEFIMRURunTimeConfigurations<int, int, int>.GetYarnIMRUConfiguration(),
                        externalConfig
                    }).
                    GetInstance<MapperCount.MapperCount>();
            }

            var result = tested.Run(numNodes - 1);
        }
    }
}
