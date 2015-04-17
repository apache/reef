using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Static class containing functions to read and write some basic types from/to stream
    /// </summary>
    public static class AuxillaryStreamingFunctions
    {
        /// <summary>
        /// Read bytes from the stream.
        /// </summary>
        /// <param name="stream">Stream fromwhich to read</param>
        /// <param name="bytesToRead">Number of bytes to read</param>
        /// <param name="buffer">byte buffer to read in to</param>
        /// <param name="offset">offset in the buffer where we should start writing</param>
        /// <returns>The number of bytes read if success otherwise -1</returns>
        internal static int ReadBytes(Stream stream, int bytesToRead, byte[] buffer, int offset = 0)
        {
            int totalBytesRead = 0;
            while (totalBytesRead < bytesToRead)
            {
                int bytesRead = stream.Read(buffer, offset + totalBytesRead, bytesToRead - totalBytesRead);
                if (bytesRead == 0)
                {
                    // Read timed out or connection was closed
                    return -1;
                }

                totalBytesRead += bytesRead;
            }

            return bytesToRead;
        }

        /// <summary>
        /// Read bytes asynchronously from the stream.
        /// </summary>
        /// <param name="stream">Stream fromwhich to read</param>
        /// <param name="bytesToRead">Number of bytes to read</param>
        /// <param name="buffer">byte buffer to read in to</param>
        /// <param name="token">the cancellation token</param>
        /// <param name="offset">offset in the buffer where we should start writing</param>
        /// <returns>The number of bytes read if success otherwise -1</returns>
        internal static async Task<int> ReadBytesAsync(Stream stream, int bytesToRead, byte[] buffer, CancellationToken token, int offset = 0)
        {
            int totalBytesRead = 0;
            while (totalBytesRead < bytesToRead)
            {
                int bytesRead = await stream.ReadAsync(buffer, totalBytesRead, bytesToRead - totalBytesRead, token);
                if (bytesRead == 0)
                {
                    // Read timed out or connection was closed
                    return -1;
                }

                totalBytesRead += bytesRead;
            }

            return bytesToRead;
        }
        
        /// <summary>
        /// Writes an integer to the stream
        /// </summary>
        /// <param name="obj">The integer to write</param>
        /// <param name="stream">The stream to write to.</param>
        public static void IntToStream(int obj, Stream stream)
        {
            stream.Write(BitConverter.GetBytes(obj), 0, sizeof(int));
        }

        /// <summary>
        /// Reads an integer from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>The read integer</returns>
        public static int? StreamToInt(Stream stream)
        {
            byte[] intBytes = new byte[sizeof(int)];
            int readBytes = ReadBytes(stream, sizeof (int), intBytes);

            if (readBytes == -1)
            {
                return null;
            }
            return BitConverter.ToInt32(intBytes, 0);
        }

        /// <summary>
        /// Writes an integer to the stream
        /// </summary>
        /// <param name="obj">The integer to write</param>
        /// <param name="stream">The stream to write to.</param>
        public static void LongToStream(long obj, Stream stream)
        {
            stream.Write(BitConverter.GetBytes(obj), 0, sizeof(long));
        }

        /// <summary>
        /// Reads an integer from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>The read integer</returns>
        public static long? StreamToLong(Stream stream)
        {
            byte[] longBytes = new byte[sizeof(long)];
            int readBytes = ReadBytes(stream, sizeof(long), longBytes);

            if (readBytes == -1)
            {
                return null;
            }
            return BitConverter.ToInt64(longBytes, 0);
        }


        /// <summary>
        /// Writes a string to the stream
        /// </summary>
        /// <param name="obj">The string to write</param>
        /// <param name="stream">The stream to write to.</param>
        public static void StringToStream(string obj, Stream stream)
        {
            var charString = obj.ToCharArray();
            byte[] byteString = new byte[charString.Length*sizeof (char)];
            IntToStream(byteString.Length, stream);
            Buffer.BlockCopy(charString, 0, byteString, 0, byteString.Length);
            stream.Write(byteString, 0, byteString.Length);
        }

        /// <summary>
        /// Reads a string from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>The read string</returns>
        public static string StreamToString(Stream stream)
        {
            int? stringLength = StreamToInt(stream);

            if (!stringLength.HasValue)
            {
                return null;
            }

            int length = stringLength.Value;
            byte[] stringByte = new byte[length];
            int readBytes = ReadBytes(stream, stringByte.Length, stringByte);

            if (readBytes == -1)
            {
                return null;
            }

            char[] stringChar = new char[stringByte.Length/sizeof (char)];
            Buffer.BlockCopy(stringByte, 0, stringChar, 0, stringByte.Length);
            return new string(stringChar);
        }


        /// <summary>
        /// Writes an integer to the stream
        /// </summary>
        /// <param name="obj">The integer to write</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        public static async Task IntToStreamAsync(int obj, Stream stream, CancellationToken token)
        {
            await stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof (int), token);
        }

        /// <summary>
        /// Reads an integer from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        /// <returns>The read integer</returns>
        public static async Task<int?> StreamToIntAsync(Stream stream, CancellationToken token)
        {
            byte[] intBytes = new byte[sizeof(int)];
            int readBytes = await ReadBytesAsync(stream, sizeof(int), intBytes, token);

            if (readBytes == -1)
            {
                return null;
            }
            return BitConverter.ToInt32(intBytes, 0);
        }

        /// <summary>
        /// Writes an integer to the stream
        /// </summary>
        /// <param name="obj">The integer to write</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        public static async Task LongToStreamAsync(long obj, Stream stream, CancellationToken token)
        {
            await stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(long), token);
        }

        /// <summary>
        /// Reads an integer from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        /// <returns>The read integer</returns>
        public static async Task<long?> StreamToLongAsync(Stream stream, CancellationToken token)
        {
            byte[] longBytes = new byte[sizeof(long)];
            int readBytes = await ReadBytesAsync(stream, sizeof(long), longBytes, token);

            if (readBytes == -1)
            {
                return null;
            }
            return BitConverter.ToInt64(longBytes, 0);
        }


        /// <summary>
        /// Writes a string to the stream
        /// </summary>
        /// <param name="obj">The string to write</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        public static async Task StringToStreamAsync(string obj, Stream stream, CancellationToken token)
        {
            var charString = obj.ToCharArray();
            byte[] byteString = new byte[charString.Length * sizeof(char)];
            await IntToStreamAsync(byteString.Length, stream, token);
            Buffer.BlockCopy(charString, 0, byteString, 0, byteString.Length);
            await stream.WriteAsync(byteString, 0, byteString.Length, token);
        }

        /// <summary>
        /// Reads a string from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="token">the cancellation token</param>
        /// <returns>The read string</returns>
        public static async Task<string> StreamToStringAsync(Stream stream, CancellationToken token)
        {
            int? stringLength = await StreamToIntAsync(stream, token);
            
            if (!stringLength.HasValue)
            {
                return null;
            }

            int length = stringLength.Value;
            byte[] stringByte = new byte[length];

            int readBytes = await ReadBytesAsync(stream, stringByte.Length, stringByte, token);

            if (readBytes == -1)
            {
                return null;
            }

            char[] stringChar = new char[stringByte.Length / sizeof(char)];
            Buffer.BlockCopy(stringByte, 0, stringChar, 0, stringByte.Length);
            string value = new string(stringChar);
            return value;
        }
    }
}
