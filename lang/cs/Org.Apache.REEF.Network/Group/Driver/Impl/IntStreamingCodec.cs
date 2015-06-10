using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Streaming codec for integer
    /// </summary>
    public class IntStreamingCodec : IStreamingCodec<int>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        public IntStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        ///<returns>The iinteger read from the reader</returns>
        public int Read(IDataReader reader)
        {
            return reader.ReadInt32();
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(int obj, IDataWriter writer)
        {
            writer.WriteInt32(obj);
        }

        ///  <summary>
        ///  Instantiate the class from the reader.
        ///  </summary>
        ///  <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The integer read from the reader</returns>
        public async Task<int> ReadAsync(IDataReader reader, CancellationToken token)
        {
            return await reader.ReadInt32Async(token);
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(int obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteInt32Async(obj, token);
        }
    }
}
