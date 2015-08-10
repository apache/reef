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
using System.Linq;
using Org.Apache.REEF.IMRU.OnREEF;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.IMRU.Examples.MapperCount;
using Org.Apache.REEF.IMRU.OnREEF.Client;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IMRU.Examples
{
    /// <summary>
    /// Runs IMRU for mapper count either in localruntime or on cluster.
    /// </summary>
    public class Run
    {
        private static void RunMapperTest(IConfiguration tcpPortConfig, bool runOnYarn, int numNodes)
        {
            IInjector injector;

            if (!runOnYarn)
            {
                injector =
                    TangFactory.GetTang()
                        .NewInjector(new[]
                        {
                            OnREEFIMRURunTimeConfigurations<int, int, int>.GetLocalIMRUConfiguration(numNodes),
                            tcpPortConfig
                        });
            }
            else
            {
                injector = TangFactory.GetTang()
                    .NewInjector(new[]
                    {
                        OnREEFIMRURunTimeConfigurations<int, int, int>.GetYarnIMRUConfiguration(),
                        tcpPortConfig
                    });
            }

            MapperCount.MapperCount tested = injector.GetInstance<MapperCount.MapperCount>();
            var result = tested.Run(numNodes - 1);
        }

        private static void RunBroadcastReduceTest(IConfiguration tcpPortConfig, bool runOnYarn, int numNodes, string[] args)
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 10;

            if (args.Length > 0)
            {
                dims = Convert.ToInt32(args[0]);
            }

            if (args.Length > 1)
            {
                chunkSize = Convert.ToInt32(args[1]);
            }

            if (args.Length > 2)
            {
                iterations = Convert.ToInt32(args[2]);
            }

            IInjector injector;

            if (!runOnYarn)
            {
                injector =
                    TangFactory.GetTang()
                        .NewInjector(new[]
                        {
                            OnREEFIMRURunTimeConfigurations<int[], int[], int[]>.GetLocalIMRUConfiguration(numNodes),
                            tcpPortConfig
                        });
            }
            else
            {
                injector = TangFactory.GetTang()
                    .NewInjector(new[]
                    {
                        OnREEFIMRURunTimeConfigurations<int, int, int>.GetYarnIMRUConfiguration(),
                        tcpPortConfig
                    });
            }
            var tested = injector.GetInstance<PipelinedBroadcastReduce.PipelinedBroadcastAndReduce>();
            tested.Run(numNodes - 1, chunkSize, iterations, dims);
        }

        private static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            string methodName = "MapperCount";
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

                if (args.Length > 4)
                {
                    methodName = args[4];
                }
            }

            var tcpPortConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof (TcpPortRangeStart),
                    startPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof (TcpPortRangeCount),
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            if (methodName.ToLower().Equals("mappercount"))
            {
                Console.WriteLine("Running Mapper count");
                RunMapperTest(tcpPortConfig, runOnYarn, numNodes);
                Console.WriteLine("Done Running Mapper count");
            }

            if (methodName.ToLower().Equals("broadcastandreduce"))
            {
                Console.WriteLine("Running Broadcast and Reduce");
                RunBroadcastReduceTest(tcpPortConfig, runOnYarn, numNodes, args.Skip(5).ToArray());
                Console.WriteLine("Done Running Broadcast and Reduce");
            }

        }
    }
}
