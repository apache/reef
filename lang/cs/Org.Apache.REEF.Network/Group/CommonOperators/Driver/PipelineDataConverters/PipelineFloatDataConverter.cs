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
    /// Used to convert the float array message to be communicated in to 
    /// pipelining amenable data and vice-versa 
    /// </summary>
    public class PipelineFloatDataConverter : IPipelineDataConverter<float[]>
    {
        private readonly int _chunkSize;

        /// <summary>
        /// Create a new PipelineFloatDataConverter object
        /// </summary>
        /// <param name="chunkSize">Size of the smaller arrays in to which 
        /// the full array is divided</param>
        [Inject]
        public PipelineFloatDataConverter([Parameter(typeof(PipelineOptions.FloatChunkSize))] int chunkSize)
        {
            _chunkSize = chunkSize;
        }

        /// <summary>
        /// Converts the original float array message to be communicated in 
        /// to a vector of pipelined float array messages.
        /// Each element of vector is communicated as a single logical unit.
        /// </summary>
        /// <param name="message">The original float array message</param>
        /// <returns>The list of pipelined float array messages</returns>
        public List<PipelineMessage<float[]>> PipelineMessage(float[] message)
        {
            List<PipelineMessage<float[]>> messageList = new List<PipelineMessage<float[]>>();
            int totalChunks = message.Length / _chunkSize;

            if (message.Length % _chunkSize != 0)
            {
                totalChunks++;
            }

            int counter = 0;
            for (int i = 0; i < message.Length; i += _chunkSize)
            {
                float[] data = new float[Math.Min(_chunkSize, message.Length - i)];
                Buffer.BlockCopy(message, i * sizeof(float), data, 0, data.Length * sizeof(float));

                if (counter == totalChunks - 1)
                {
                    messageList.Add(new PipelineMessage<float[]>(data, true));
                }
                else
                {
                    messageList.Add(new PipelineMessage<float[]>(data, false));
                }

                counter++;
            }

            return messageList;
        }

        /// <summary>
        /// Constructs the full float array message from the list of communicated 
        /// pipelined float array messages
        /// </summary>
        /// <param name="pipelineMessage">The enumerator over received pipelined float 
        /// array messages</param>
        /// <returns>The full constructed float array message</returns>
        public float[] FullMessage(List<PipelineMessage<float[]>> pipelineMessage)
        {
            int size = pipelineMessage.Select(x => x.Data.Length).Sum();
            float[] data = new float[size];
            int offset = 0;

            foreach (var message in pipelineMessage)
            {
                Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(float));
                offset += message.Data.Length * sizeof(float);
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
            .BindNamedParameter<PipelineOptions.FloatChunkSize, int>(GenericType<PipelineOptions.FloatChunkSize>.Class, _chunkSize.ToString(CultureInfo.InvariantCulture))
            .Build();
        }
    }
}
