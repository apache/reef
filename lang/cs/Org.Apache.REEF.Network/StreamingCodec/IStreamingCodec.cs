using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>
    /// Codec Interface that external users should implement to directly write to the stream
    /// </summary>
    public interface IStreamingCodec<T>
    {
        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        ///<returns>The instance of type T read from the reader</returns>
        T Read(IDataReader reader);

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object of type T to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        void Write(T obj, IDataWriter writer);

        ///  <summary>
        ///  Instantiate the class from the reader.
        ///  </summary>
        ///  <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The instance of type T read from the reader</returns>
        Task<T> ReadAsync(IDataReader reader, CancellationToken token);

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object of type T to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        Task WriteAsync(T obj, IDataWriter writer, CancellationToken token);
    }
}
