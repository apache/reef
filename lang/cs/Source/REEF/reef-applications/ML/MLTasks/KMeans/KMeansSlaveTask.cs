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

using System.Collections.Generic;
using System.Linq;
using Org.Apache.Reef.IO.Network.Group.Operators;
using Org.Apache.Reef.IO.Network.Group.Task;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.Applications.MLTasks.KMeans
{
    public class KMeansSlaveTask : ITask
    {
        private static Logger _logger = Logger.GetLogger(typeof(KMeansSlaveTask));
        private int _clustersNum;
        private IMpiClient _mpiClient;
        private ICommunicationGroupClient _commGroup;
        private IBroadcastReceiver<Centroids> _dataBroadcastReceiver;
        private IBroadcastReceiver<ControlMessage> _controlBroadcastReceiver;
        private IReduceSender<ProcessedResults> _partialMeansSender;
        private DataPartitionCache _dataPartition;

        [Inject]
        public KMeansSlaveTask(
            DataPartitionCache dataPartition,
            [Parameter(typeof(KMeansConfiguratioinOptions.TotalNumEvaluators))] int clustersNumber,
            IMpiClient mpiClient)
        {
            using (_logger.LogFunction("KMeansSlaveTask::KMeansSlaveTask"))
            {
                _dataPartition = dataPartition;
                _mpiClient = mpiClient;
                _clustersNum = clustersNumber;
                _commGroup = _mpiClient.GetCommunicationGroup(Constants.KMeansCommunicationGroupName);
                _dataBroadcastReceiver = _commGroup.GetBroadcastReceiver<Centroids>(Constants.CentroidsBroadcastOperatorName);
                _partialMeansSender = _commGroup.GetReduceSender<ProcessedResults>(Constants.MeansReduceOperatorName);
                _controlBroadcastReceiver = _commGroup.GetBroadcastReceiver<ControlMessage>(Constants.ControlMessageBroadcastOperatorName);
            }      
        }

        public byte[] Call(byte[] memento)
        {
            while (true)
            {
                if (_controlBroadcastReceiver.Receive() == ControlMessage.STOP)
                {
                    break;
                }
                Centroids centroids = _dataBroadcastReceiver.Receive();
                // we compute the loss here before data is relabled, this does not reflect the latest clustering result at the end of current iteration, 
                // but it will save another round of group communications in each iteration
                _logger.Log(Level.Info, "Received centroids from master: " + centroids);
                _dataPartition.LabelData(centroids);
                ProcessedResults partialMeans = new ProcessedResults(ComputePartialMeans(), ComputeLossFunction(centroids, _dataPartition.DataVectors));
                _logger.Log(Level.Info, "Sending partial means: " + partialMeans);
                _partialMeansSender.Send(partialMeans);
            }

            return null;
        }

        public void Dispose()
        {
            _mpiClient.Dispose();
        }

        private List<PartialMean> ComputePartialMeans()
        {
            _logger.Log(Level.Verbose, "cluster number " + _clustersNum);
            List<PartialMean> partialMeans = new PartialMean[_clustersNum].ToList();
            for (int i = 0; i < _clustersNum; i++)
            {
                List<DataVector> slices = _dataPartition.DataVectors.Where(d => d.Label == i).ToList();
                DataVector average = new DataVector(_dataPartition.DataVectors[0].Dimension);

                if (slices.Count > 1)
                {
                    average = DataVector.Mean(slices);
                }
                average.Label = i;
                partialMeans[i] = new PartialMean(average, slices.Count);
                _logger.Log(Level.Info, "Adding to partial means list: " + partialMeans[i]);
            }
            return partialMeans;
        }

        private float ComputeLossFunction(Centroids centroids, List<DataVector> labeledData)
        {
            float d = 0;
            for (int i = 0; i < centroids.Points.Count; i++)
            {
                DataVector centroid = centroids.Points[i];
                List<DataVector> slice = labeledData.Where(v => v.Label == centroid.Label).ToList();
                d += centroid.DistanceTo(slice);
            }
            return d;
        }
    }
}
