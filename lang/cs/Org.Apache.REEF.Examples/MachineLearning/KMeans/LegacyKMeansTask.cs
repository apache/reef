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

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    /// <summary>
    /// This is the legacy KmeansTask implemented when group communications are not available
    /// It is still being used for plain KMeans without REEF, we probably want to refactor it later 
    /// to reflect that
    /// </summary>
    public class LegacyKMeansTask
    {
        private readonly int _clustersNum;
        private readonly DataPartitionCache _dataPartition;
        private readonly string _kMeansExecutionDirectory;

        private Centroids _centroids;
        private List<PartialMean> _partialMeans;

        [Inject]
        public LegacyKMeansTask(
            DataPartitionCache dataPartition,
            [Parameter(Value = typeof(KMeansConfiguratioinOptions.K))] int clustersNumber,
            [Parameter(Value = typeof(KMeansConfiguratioinOptions.ExecutionDirectory))] string executionDirectory)
        {
            _dataPartition = dataPartition;
            _clustersNum = clustersNumber;
            _kMeansExecutionDirectory = executionDirectory;
            if (_centroids == null)
            {
                string centroidFile = Path.Combine(_kMeansExecutionDirectory, Constants.CentroidsFile);
                _centroids = new Centroids(DataPartitionCache.ReadDataFile(centroidFile));
            }
        }

        public static float ComputeLossFunction(List<DataVector> centroids, List<DataVector> labeledData)
        {
            float d = 0;
            for (int i = 0; i < centroids.Count; i++)
            {
                DataVector centroid = centroids[i];
                List<DataVector> slice = labeledData.Where(v => v.Label == centroid.Label).ToList();
                d += centroid.DistanceTo(slice);
            }
            return d;
        }

        public byte[] CallWithWritingToFileSystem(byte[] memento)
        {
            string centroidFile = Path.Combine(_kMeansExecutionDirectory, Constants.CentroidsFile);
            _centroids = new Centroids(DataPartitionCache.ReadDataFile(centroidFile));

            _dataPartition.LabelData(_centroids);
            _partialMeans = ComputePartialMeans();

            // should be replaced with Group Communication
            using (StreamWriter writer = new StreamWriter(
                    File.OpenWrite(Path.Combine(_kMeansExecutionDirectory, Constants.DataDirectory, Constants.PartialMeanFilePrefix + _dataPartition.Partition))))
            {
                for (int i = 0; i < _partialMeans.Count; i++)
                {
                    writer.WriteLine(_partialMeans[i].ToString());
                }
                writer.Close();
            }

            return null;
        }

        public List<PartialMean> ComputePartialMeans()
        {
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
            }
            return partialMeans;
        }

        public void Dispose()
        {
        }
    }
}
