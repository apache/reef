using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using System.Globalization;

namespace Org.Apache.REEF.Network.Group.CommonOperators.Driver.PipelineDataConverters
{
    /// <summary>
    /// Used to convert the double array message to be communicated in to 
    /// pipelining amenable data and vice-versa 
    /// </summary>
    public class PipelineDoubleDataConverter : IPipelineDataConverter<double[]>
    {
        private readonly int _chunkSize;

        /// <summary>
        /// Create a new PipelineDoubleDataConverter object
        /// </summary>
        /// <param name="chunkSize">Size of the smaller arrays in to which 
        /// the full array is divided</param>
        [Inject]
        public PipelineDoubleDataConverter([Parameter(typeof(PipelineOptions.DoubleChunkSize))] int chunkSize)
        {
            _chunkSize = chunkSize;
        }

        /// <summary>
        /// Converts the original double array message to be communicated in 
        /// to a vector of pipelined double array messages.
        /// Each element of vector is communicated as a single logical unit.
        /// </summary>
        /// <param name="message">The original double array message</param>
        /// <returns>The list of pipelined double array messages</returns>
        public List<PipelineMessage<double[]>> PipelineMessage(double[] message)
        {
            List<PipelineMessage<double[]>> messageList = new List<PipelineMessage<double[]>>();
            int totalChunks = message.Length / _chunkSize;

            if (message.Length % _chunkSize != 0)
            {
                totalChunks++;
            }

            int counter = 0;
            for (int i = 0; i < message.Length; i += _chunkSize)
            {
                double[] data = new double[Math.Min(_chunkSize, message.Length - i)];
                Buffer.BlockCopy(message, i * sizeof(double), data, 0, data.Length * sizeof(double));

                if (counter == totalChunks - 1)
                {
                    messageList.Add(new PipelineMessage<double[]>(data, true));
                }
                else
                {
                    messageList.Add(new PipelineMessage<double[]>(data, false));
                }

                counter++;
            }

            return messageList;
        }

        /// <summary>
        /// Constructs the full double array message from the list of communicated 
        /// pipelined double array messages
        /// </summary>
        /// <param name="pipelineMessage">The enumerator over received pipelined double 
        /// array messages</param>
        /// <returns>The full constructed double array message</returns>
        public double[] FullMessage(List<PipelineMessage<double[]>> pipelineMessage)
        {
            int size = pipelineMessage.Select(x => x.Data.Length).Sum();
            double[] data = new double[size];
            int offset = 0;

            foreach (var message in pipelineMessage)
            {
                Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(double));
                offset += message.Data.Length * sizeof(double);
            }

            return data;
        }

        /// <summary>
        /// Constructs the configuration of the class. Basically the arguments of the class like chunksize
        /// </summary>
        /// <returns>The configuration for this data converter class</returns>
        public IConfiguration GetConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
            .BindNamedParameter<PipelineOptions.DoubleChunkSize, int>(GenericType<PipelineOptions.DoubleChunkSize>.Class, _chunkSize.ToString(CultureInfo.InvariantCulture))
            .Build();
        }
    }
}
