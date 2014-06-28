#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				RunningTaskClr2Java::RunningTaskClr2Java(JNIEnv *env, jobject jobjectRunningTask)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectRunningTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectRunningTask));

					fprintf(stdout, "RunningTaskClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "RunningTaskClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "RunningTaskClr2Java _jobjectRunningTask %p\n", _jobjectRunningTask); fflush (stdout);
				}

				IActiveContextClr2Java^ RunningTaskClr2Java::GetActiveContext()
				{
					fprintf(stdout, "RunningTaskClr2Java::GetActiveContext\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
					jfieldID jidActiveContext = env->GetFieldID(jclassRunningTask, "jactiveContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
					jobject jobjectActiveContext = env->GetObjectField(_jobjectRunningTask, jidActiveContext);

					fprintf(stdout, "RunningTaskClr2Java jidActiveContext %p\n", jidActiveContext); fflush (stdout);
					fprintf(stdout, "RunningTaskClr2Java jobjectActiveContext %p\n", jobjectActiveContext); fflush (stdout);
					return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
				}

				String^ RunningTaskClr2Java::GetString()
				{
					fprintf(stdout, "RunningTaskClr2Java::GetString\n"); fflush (stdout);															
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassRunningTask = env->GetObjectClass (_jobjectRunningTask);
					jmethodID jmidGetRunningTaskString= env->GetMethodID(jclassRunningTask, "getRunningTaskString", "()Ljava/lang/String;");	

					fprintf(stdout, "RunningTaskClr2Java jclassRunningTask %p\n", jclassRunningTask); fflush (stdout);
					fprintf(stdout, "RunningTaskClr2Java jmidGetRunningTaskString %p\n", jmidGetRunningTaskString); fflush (stdout);

					if(jmidGetRunningTaskString == NULL)
					{
						fprintf(stdout, " jmidGetRunningTaskString is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jRunningTaskString = (jstring)env -> CallObjectMethod(
						_jobjectRunningTask, 
						jmidGetRunningTaskString);
					return ManagedStringFromJavaString(env, jRunningTaskString);
				}

				void RunningTaskClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "RunningTaskClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectRunningTask);
				}
			}
		}
	}
}