#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				TaskMessageClr2Java::TaskMessageClr2Java(JNIEnv *env, jobject jtaskMessage)
				{
					_env = env;
					_jobjectTaskMessage = jtaskMessage;
					_jclassTaskMessage = _env->GetObjectClass (_jobjectTaskMessage);

					fprintf(stdout, "AllocatedEvaluatorClr2Java _env %p\n", _env); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectTaskMessage %p\n", _jobjectTaskMessage); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jclassTaskMessage %p\n", _jclassTaskMessage); fflush (stdout);
				}
			}
		}
	}
}