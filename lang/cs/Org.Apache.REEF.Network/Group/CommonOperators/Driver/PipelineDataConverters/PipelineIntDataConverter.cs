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
    /// Used to convert the integer array message to be communicated in to 
    /// pipelining amenable data and vice-versa 
    /// </summary>
    public class PipelineIntDataConverter : IPipelineDataConverter<int[]>
    {
        private readonly int _chunkSize;

        /// <summary>
        /// Create a new PipelineIntDataConverter object
        /// </summary>
        /// <param name="chunkSize">Size of the smaller arrays in to which 
        /// the full array is divided</param>
        [Inject]
        public PipelineIntDataConverter([Parameter(typeof(PipelineOptions.IntegerChunkSize))] int chunkSize)
        {
            _chunkSize = chunkSize;
        }

        /// <summary>
        /// Converts the original integer array message to be communicated in 
        /// to a vector of pipelined integer array messages.
        /// Each element of vector is communicated as a single logical unit.
        /// </summary>
        /// <param name="message">The original integer array message</param>
        /// <returns>The list of pipelined integer array messages</returns>
        public List<PipelineMessage<int[]>> PipelineMessage(int[] message)
        {
            List<PipelineMessage<int[]>> messageList = new List<PipelineMessage<int[]>>();
            int totalChunks = message.Length / _chunkSize;

            if (message.Length % _chunkSize != 0)
            {
                totalChunks++;
            }

            int counter = 0;
            for (int i = 0; i < message.Length; i += _chunkSize)
            {
                int[] data = new int[Math.Min(_chunkSize, message.Length - i)];
                Buffer.BlockCopy(message, i * sizeof(int), data, 0, data.Length * sizeof(int));

                if (counter == totalChunks - 1)
                {
                    messageList.Add(new PipelineMessage<int[]>(data, true));
                }
                else
                {
                    messageList.Add(new PipelineMessage<int[]>(data, false));
                }

                counter++;
            }

            return messageList;
        }

        /// <summary>
        /// Constructs the full integer array message from the list of communicated 
        /// pipelined integer array messages
        /// </summary>
        /// <param name="pipelineMessage">The enumerator over received pipelined integer 
        /// array messages</param>
        /// <returns>The full constructed integer array message</returns>
        public int[] FullMessage(List<PipelineMessage<int[]>> pipelineMessage)
        {
            int size = pipelineMessage.Select(x => x.Data.Length).Sum();
            int[] data = new int[size];
            int offset = 0;

            foreach (var message in pipelineMessage)
            {
                Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(int));
                offset += message.Data.Length * sizeof(int);
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
            .BindNamedParameter<PipelineOptions.IntegerChunkSize, int>(GenericType<PipelineOptions.IntegerChunkSize>.Class, _chunkSize.ToString(CultureInfo.InvariantCulture))
            .Build();
        }
    }
}
