using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    public class TestEvent : IWritable, IType
    {
        public TestEvent()
        {
            ClassType = this.GetType();
        }

        public TestEvent(string message)
        {
            Message = message;
            ClassType = this.GetType();
        }

        public string Message { get; set; }

        public override string ToString()
        {
            return "TestEvent: " + Message;
        }

        public void Read(Stream stream, params object[] optionalParameters)
        {
            WritableString stringClass = new WritableString();
            stringClass.Read(stream);
            Message = stringClass.Data;
        }

        public void Write(Stream stream)
        {
            WritableString stringClass = new WritableString(Message);
            stringClass.Write(stream);
        }

        public async Task ReadAsync(Stream stream, CancellationToken token, params object[] optionalParameters)
        {
            WritableString stringClass = new WritableString();
            await stringClass.ReadAsync(stream, token);
            Message = stringClass.Data;
        }

        public async Task WriteAsync(Stream stream, CancellationToken token)
        {
            WritableString stringClass = new WritableString(Message);
            await stringClass.WriteAsync(stream, token);
        }

        public Type ClassType { get; set; }
    }
}