using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface that classes should implement if they need to be readable to and writable 
    /// from the stream. It is assumed that the classes inheriting this interface will have a 
    /// default empty constructor
    /// </summary>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public interface IWritable
    {
        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        void Read(Stream stream);

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        void Write(Stream stream);

        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        /// <param name="token">The cancellation token</param>
        Task ReadAsync(Stream stream, CancellationToken token);

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        /// <param name="token">The cancellation token</param>
        Task WriteAsync(Stream stream, CancellationToken token);
    }
}
