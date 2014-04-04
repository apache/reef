using System;

namespace Microsoft.Reef.Interop
{
    public interface IAllocatedEvaluaotrClr2Java
    {
        void SubmitContextAndTask(string contextConfigStr, string taskConfigStr);

        void SubmitContext(string contextConfigStr);
    }
}
