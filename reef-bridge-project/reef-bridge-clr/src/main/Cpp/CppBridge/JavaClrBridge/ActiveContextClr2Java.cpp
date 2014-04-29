#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				ActiveContextClr2Java::ActiveContextClr2Java(JNIEnv *env, jobject jallocatedEvaluator)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectActiveContext = jallocatedEvaluator;

					fprintf(stdout, "ActiveContextClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jobjectActiveContext %p\n", _jobjectActiveContext); fflush (stdout);
				}

				void ActiveContextClr2Java::SubmitTask( String^ taskConfigStr)
				{
					fprintf(stdout, "ActiveContextClr2Java::SubmitTask"); fflush (stdout);					
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassActiveContext = env->GetObjectClass (_jobjectActiveContext);
					jmethodID jmidSubmitTask = env->GetMethodID(jclassActiveContext, "submitTaskString", "(Ljava/lang/String;)V");

					fprintf(stdout, "ActiveContextClr2Java jclassActiveContext %p\n", jclassActiveContext); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java jmidSubmitTask %p\n", jmidSubmitTask); fflush (stdout);

					if(jmidSubmitTask == NULL)
					{
						fprintf(stdout, " jmidSubmitTask is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectActiveContext, 
						jmidSubmitTask, 
						JavaStringFromManagedString(env, taskConfigStr));
				}
			}
		}
	}
}