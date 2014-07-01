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

				void RunningTaskClr2Java::Send(array<byte>^ message)
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
					jmethodID jmidsend = env->GetMethodID(jclassRunningTask, "send", "([B)V");

					fprintf(stdout, "RunningTaskClr2Java jclassRunningTask %p\n", jclassRunningTask); fflush (stdout);
					fprintf(stdout, "RunningTaskClr2Java jmidsend %p\n", jmidsend); fflush (stdout);

					if(jmidsend == NULL)
					{
						fprintf(stdout, " jmidsend is NULL\n"); fflush (stdout);
						return;
					}
					env->CallObjectMethod(
						_jobjectRunningTask, 
						jmidsend,
						JavaByteArrayFromManagedByteArray(env, message));					
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