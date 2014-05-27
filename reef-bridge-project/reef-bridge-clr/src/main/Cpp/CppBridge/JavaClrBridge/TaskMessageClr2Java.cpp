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
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectTaskMessage = reinterpret_cast<jobject>(env->NewGlobalRef(jtaskMessage));

					fprintf(stdout, "TaskMessageClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "TaskMessageClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "TaskMessageClr2Java _jobjectTaskMessage %p\n", _jobjectTaskMessage); fflush (stdout);
				}

				void TaskMessageClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "TaskMessageClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectTaskMessage);
				}
			}
		}
	}
}