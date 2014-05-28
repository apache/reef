#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				CompletedTaskClr2Java::CompletedTaskClr2Java(JNIEnv *env, jobject jobjectCompletedTask)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectCompletedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectCompletedTask));

					fprintf(stdout, "CompletedTaskClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "CompletedTaskClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "CompletedTaskClr2Java _jobjectCompletedTask %p\n", _jobjectCompletedTask); fflush (stdout);

					jclass jclassCompletedTask = env->GetObjectClass (_jobjectCompletedTask);
					jfieldID jidTaskId = env->GetFieldID(jclassCompletedTask, "taskId", "Ljava/lang/String;");
					_jstringId = (jstring)env->GetObjectField(_jobjectCompletedTask, jidTaskId);
					_jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringId));
				}

				void CompletedTaskClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "CompletedTaskClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectCompletedTask);
				}

				IActiveContextClr2Java^ CompletedTaskClr2Java::GetActiveContext()
				{
					fprintf(stdout, "CompletedTaskClr2Java::GetActiveContext\n"); fflush (stdout);																				
					JNIEnv *env = RetrieveEnv(_jvm);


					jclass jclassCompletedTask = env->GetObjectClass (_jobjectCompletedTask);
					jfieldID jidActiveContext = env->GetFieldID(jclassCompletedTask, "jactiveContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
					jobject jobjectActiveContext = env->GetObjectField(_jobjectCompletedTask, jidActiveContext);

					fprintf(stdout, "CompletedTaskClr2Java jobjectActiveContext %p\n", jobjectActiveContext); fflush (stdout);
					return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
				}

				String^ CompletedTaskClr2Java::GetId()
				{
					fprintf(stdout, "CompletedTaskClr2Java::GetId\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringId);
				}
			}
		}
	}
}