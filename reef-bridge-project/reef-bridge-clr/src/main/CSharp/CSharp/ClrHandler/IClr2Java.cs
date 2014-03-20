using System;

namespace Microsoft.Reef.Interop
{
    public interface IClr2Java
    {
        void Emanager_submit(byte[] bytes);

        void AllocatedEvaluatorSubmitContextAndTask(string contextConfigStr, string taskConfigStr);
    }
}
