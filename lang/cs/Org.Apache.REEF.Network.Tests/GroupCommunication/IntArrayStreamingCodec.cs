using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    /// <summary>
    /// Streaming codec for integer
    /// </summary>
    public class IntArrayStreamingCodec : IStreamingCodec<int[]>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        public IntArrayStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        ///<returns>The iinteger read from the reader</returns>
        public int[] Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[] buffer = new byte[sizeof(int)*length];
            reader.Read(ref buffer, 0, buffer.Length);
            int[] intArr = new int[length];
            Buffer.BlockCopy(buffer, 0, intArr, 0, buffer.Length);
            return intArr;
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(int[] obj, IDataWriter writer)
        {
            writer.WriteInt32(obj.Length);
            byte[] buffer = new byte[sizeof(int) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        ///  <summary>
        ///  Instantiate the class from the reader.
        ///  </summary>
        ///  <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The integer read from the reader</returns>
        public async Task<int[]> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            byte[] buffer = new byte[sizeof(int) * length];
            await reader.ReadAsync(buffer, 0, buffer.Length, token);
            int[] intArr = new int[length];
            Buffer.BlockCopy(buffer, 0, intArr, 0, sizeof(int) * length);
            return intArr;
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(int[] obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteInt32Async(obj.Length, token);
            byte[] buffer = new byte[sizeof(int) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, sizeof(int) * obj.Length);
            await writer.WriteAsync(buffer, 0, buffer.Length, token);
        }
    }
}
