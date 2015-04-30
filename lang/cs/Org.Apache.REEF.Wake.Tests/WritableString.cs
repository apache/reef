using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.Tests
{
    /// <summary>
    /// Writable wrapper around the string class
    /// </summary>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableString : IWritable
    {
        /// <summary>
        /// Returns the actual string data
        /// </summary>
        public string Data { get; set; }
        
        /// <summary>
        /// Empty constructor for instantiation with reflection
        /// </summary>
        public WritableString()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">The string data</param>
        public WritableString(string data)
        {
            Data = data;
        }

        /// <summary>
        /// Reads the string from the stream
        /// </summary>
        /// <param name="stream">stream from which reading is done</param>
        public void Read(Stream stream)
        {
            Data = AuxillaryStreamingFunctions.StreamToString(stream);
        }

        /// <summary>
        /// Writes the string to the stream
        /// </summary>
        /// <param name="stream">The stream to which string is written</param>
        public void Write(Stream stream)
        {
            AuxillaryStreamingFunctions.StringToStream(Data, stream);
        }

        /// <summary>
        /// Reads the string from the stream
        /// </summary>
        /// <param name="stream">stream from which reading is done</param>
        /// <param name="token">the cancellation token</param>
        public async Task ReadAsync(Stream stream, CancellationToken token)
        {
            Data = await AuxillaryStreamingFunctions.StreamToStringAsync(stream, token);
        }

        /// <summary>
        /// Writes the string to the stream
        /// </summary>
        /// <param name="stream">The stream to which string is written</param>
        /// <param name="token">the cancellation token</param>
        public async Task WriteAsync(Stream stream, CancellationToken token)
        {
            await AuxillaryStreamingFunctions.StringToStreamAsync(Data, stream, token);
        }
    }
}
