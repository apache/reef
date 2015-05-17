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
    public class CodecToStreamingCodec<T> : IStreamingCodec<T>
    {
        private ICodec<T> _codec;
        public CodecToStreamingCodec(ICodec<T> codec)
        {
            _codec = codec;
        } 

        public T Decode(Stream stream)
        {
            int? length = AuxillaryStreamingFunctions.StreamToInt(stream);

            if (!length.HasValue)
            {
                return default(T);
            }

            byte[] encoding = new byte[length.Value];
            stream.Read(encoding, 0, length.Value);
            return _codec.Decode(encoding);
        }

        public void Encode(T obj, Stream stream)
        {
            byte[] encoding = _codec.Encode(obj);
            AuxillaryStreamingFunctions.IntToStream(encoding.Length, stream);
            stream.Write(encoding, 0, encoding.Length);
        }

        public async Task<T> DecodeAsync(Stream stream, CancellationToken token)
        {
            int? length = await AuxillaryStreamingFunctions.StreamToIntAsync(stream, token);

            if (!length.HasValue)
            {
                return default(T);
            }
            byte[] encoding = new byte[length.Value];
            await stream.ReadAsync(encoding, 0, length.Value, token);
            return _codec.Decode(encoding);
        }

        public async Task EncodeAsync(T obj, Stream stream, CancellationToken token)
        {
            byte[] encoding = _codec.Encode(obj);
            await AuxillaryStreamingFunctions.IntToStreamAsync(encoding.Length, stream, token);
            await stream.WriteAsync(encoding, 0, encoding.Length, token);
        }
    }

    /// <summary>
    /// Codec Interface that external users should implement to directly write to the stream
    /// </summary>
    public static class CodecConversion
    {
        public static IStreamingCodec<T> ToStreamingCodec<T>(ICodec<T> codec)
        {
            return new CodecToStreamingCodec<T>(codec);
        } 
    }
}
