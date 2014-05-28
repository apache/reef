#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				FailedTaskClr2Java::FailedTaskClr2Java(JNIEnv *env, jobject jobjectFailedTask)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectFailedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedTask));

					fprintf(stdout, "FailedTaskClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "FailedTaskClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "FailedTaskClr2Java _jobjectFailedTask %p\n", _jobjectFailedTask); fflush (stdout);
				}

				IActiveContextClr2Java^ FailedTaskClr2Java::GetActiveContext()
				{
					fprintf(stdout, "FailedTaskClr2Java::GetActiveContext"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassFailedTask = env->GetObjectClass(_jobjectFailedTask);
					jfieldID jidActiveContext = env->GetFieldID(jclassFailedTask, "jactiveContext", "Lcom.microsoft.reef.javabridge/ActiveContextBridge;");
					jobject jobjectActiveContext = env->GetObjectField(_jobjectFailedTask, jidActiveContext);

					fprintf(stdout, "FailedTaskClr2Java jidActiveContext %p\n", jidActiveContext); fflush (stdout);
					fprintf(stdout, "FailedTaskClr2Java jobjectActiveContext %p\n", jobjectActiveContext); fflush (stdout);
					return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
				}

				String^ FailedTaskClr2Java::GetString()
				{
					fprintf(stdout, "FailedTaskClr2Java::GetActiveContext"); fflush (stdout);															
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassFailedTask = env->GetObjectClass (_jobjectFailedTask);
					jmethodID jmidGetFailedTaskString= env->GetMethodID(jclassFailedTask, "getFailedTaskString", "()Ljava/lang/String;");	

					fprintf(stdout, "FailedTaskClr2Java jclassFailedTask %p\n", jclassFailedTask); fflush (stdout);
					fprintf(stdout, "FailedTaskClr2Java jmidGetFailedTaskString %p\n", jmidGetFailedTaskString); fflush (stdout);

					if(jmidGetFailedTaskString == NULL)
					{
						fprintf(stdout, " jmidGetFailedTaskString is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jFailedTaskString = (jstring)env -> CallObjectMethod(
						_jobjectFailedTask, 
						jmidGetFailedTaskString);
					return ManagedStringFromJavaString(env, jFailedTaskString);
				}

				void FailedTaskClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "FailedTaskClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectFailedTask);
				}
			}
		}
	}
}