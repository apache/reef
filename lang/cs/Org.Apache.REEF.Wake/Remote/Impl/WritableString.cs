using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Writable and Type wrapper around the string class
    /// </summary>
    public class WritableString : IWritable, IType
    {
        /// <summary>
        /// Returns the actual string data
        /// </summary>
        public string Data;
        
        /// <summary>
        /// Empty constructor for instantiation with reflection
        /// </summary>
        public WritableString()
        {
            ClassType = GetType();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">The string data</param>
        public WritableString(string data)
        {
            Data = data;
            ClassType = GetType();
        }

        /// <summary>
        /// Reads the string from the stream
        /// </summary>
        /// <param name="stream">stream from which reading is done</param>
        /// <param name="optionalParameters"></param>
        public void Read(Stream stream, params object[] optionalParameters)
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
        /// <param name="optionalParameters"></param>
        public async Task ReadAsync(Stream stream, CancellationToken token, params object[] optionalParameters)
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

        /// <summary>
        /// The class type property from IType
        /// </summary>
        public Type ClassType { get; set; }
    }
}
