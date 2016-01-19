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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    public class KMeansMasterTask : ITask
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(KMeansMasterTask));

        private int _iteration = 0;

        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastSender<Centroids> _dataBroadcastSender;
        private readonly IBroadcastSender<ControlMessage> _controlBroadcastSender;
        private readonly IReduceReceiver<ProcessedResults> _meansReducerReceiver;
        private readonly string _kMeansExecutionDirectory;
        private Centroids _centroids;
        private bool _isInitialIteration;

        [Inject]
        public KMeansMasterTask(
            [Parameter(typeof(KMeansConfiguratioinOptions.TotalNumEvaluators))] int totalNumEvaluators,
            [Parameter(Value = typeof(KMeansConfiguratioinOptions.ExecutionDirectory))] string executionDirectory,
            IGroupCommClient groupCommClient)
        {
            using (_logger.LogFunction("KMeansMasterTask"))
            {
                if (totalNumEvaluators <= 1)
                {
                    throw new ArgumentException("There must be more than 1 Evaluators in total, but the total evaluators number provided is " + totalNumEvaluators);
                }
                _commGroup = groupCommClient.GetCommunicationGroup(Constants.KMeansCommunicationGroupName);
                _dataBroadcastSender = _commGroup.GetBroadcastSender<Centroids>(Constants.CentroidsBroadcastOperatorName);
                _meansReducerReceiver = _commGroup.GetReduceReceiver<ProcessedResults>(Constants.MeansReduceOperatorName);
                _controlBroadcastSender = _commGroup.GetBroadcastSender<ControlMessage>(Constants.ControlMessageBroadcastOperatorName);
                _kMeansExecutionDirectory = executionDirectory;
                _isInitialIteration = true;
            }
        }

        public byte[] Call(byte[] memento)
        {
            // TODO: this belongs to dedicated dataloader layer, will refactor once we have that
            string centroidFile = Path.Combine(_kMeansExecutionDirectory, Constants.CentroidsFile);
            _centroids = new Centroids(DataPartitionCache.ReadDataFile(centroidFile));

            float loss = float.MaxValue;
            float newLoss;

            while (true)
            {
                if (_isInitialIteration)
                {
                    // broadcast initial centroids to all slave nodes
                    _logger.Log(Level.Info, "Broadcasting INITIAL centroids to all slave nodes: " + _centroids);
                    _isInitialIteration = false;
                }
                else
                {
                    ProcessedResults results = _meansReducerReceiver.Reduce();
                    _centroids = new Centroids(results.Means.Select(m => m.Mean).ToList());
                    _logger.Log(Level.Info, "Broadcasting new centroids to all slave nodes: " + _centroids);
                    newLoss = results.Loss;
                    _logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "The new loss value {0} at iteration {1} ", newLoss, _iteration));
                    if (newLoss > loss)
                    {
                        _controlBroadcastSender.Send(ControlMessage.STOP);
                        throw new InvalidOperationException(
                            string.Format(CultureInfo.InvariantCulture, "The new loss {0} is larger than previous loss {1}, while loss function must be monotonically decreasing across iterations", newLoss, loss));
                    }
                    else if (newLoss.Equals(loss))
                    {
                        _logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "KMeans clustering has converged with a loss value of {0} at iteration {1} ", newLoss, _iteration));
                        break;
                    }
                    else
                    {
                        loss = newLoss;
                    }
                }          
                _controlBroadcastSender.Send(ControlMessage.RECEIVE);
                _dataBroadcastSender.Send(_centroids); 
                _iteration++;
            }
            _controlBroadcastSender.Send(ControlMessage.STOP);
            return null;
        }

        public void Dispose()
        {
        }

        public class AggregateMeans : IReduceFunction<ProcessedResults>
        {
            [Inject]
            public AggregateMeans()
            {
            }

            public ProcessedResults Reduce(IEnumerable<ProcessedResults> elements)
            {
                List<PartialMean> aggregatedMeans = new List<PartialMean>();
                List<PartialMean> totalList = new List<PartialMean>();
                float aggregatedLoss = 0;

                foreach (var element in elements)
                {
                    totalList.AddRange(element.Means);
                    aggregatedLoss += element.Loss;
                }

                // we infer the value of K from the labeled data
                int clustersNum = totalList.Max(p => p.Mean.Label) + 1;
                for (int i = 0; i < clustersNum; i++)
                {
                    List<PartialMean> means = totalList.Where(m => m.Mean.Label == i).ToList();
                    aggregatedMeans.Add(new PartialMean(PartialMean.AggreatedMean(means), means.Count));
                }

                ProcessedResults returnMeans = new ProcessedResults(aggregatedMeans, aggregatedLoss);

                _logger.Log(Level.Info, "The true means aggregated by the reduce function: " + returnMeans);
                return returnMeans;
            }
        }
    }
}
