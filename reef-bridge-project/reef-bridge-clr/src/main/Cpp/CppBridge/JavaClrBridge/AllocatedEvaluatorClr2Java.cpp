#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator, jstring contextConfigString, jstring taskConfigString)
			{
				_env = env;
				_jobjectAllocatedEvaluator = jallocatedEvaluator;
				_jclassAllocatedEvaluator =  _env->GetObjectClass (_jobjectAllocatedEvaluator);

				_jmidSubmit = env->GetMethodID(_jclassAllocatedEvaluator, "close", "()V");	
				fprintf(stdout, "previous AllocatedEvaluatorClr2Java _jmidSubmit previous %p\n", _jmidSubmit); fflush (stdout);

				_jmidSubmit = env->GetMethodID(_jclassAllocatedEvaluator, "submitContextAndTasksString", "(Ljava/lang/String;Ljava/lang/String;)V");	
				fprintf(stdout, "AllocatedEvaluatorClr2Java _env %p\n", _env); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectAllocatedEvaluator %p\n", _jobjectAllocatedEvaluator); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jclassAllocatedEvaluator %p\n", _jclassAllocatedEvaluator); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmit %p\n", _jmidSubmit); fflush (stdout);

			}
			void AllocatedEvaluatorClr2Java::Emanager_submit(array<byte>^ bytes)
			{				
				Console::WriteLine("Emanager_submit not implemented");
			}

			void AllocatedEvaluatorClr2Java::AllocatedEvaluatorSubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
			{
				_env -> CallObjectMethod(
					_jobjectAllocatedEvaluator, 
					_jmidSubmit, 
					JavaStringFromManagedString(_env, contextConfigStr), 
					JavaStringFromManagedString(_env, taskConfigStr));
			}
		}
	}
}