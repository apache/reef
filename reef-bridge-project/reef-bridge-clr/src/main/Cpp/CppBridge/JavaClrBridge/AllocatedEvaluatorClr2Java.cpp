#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator)
			{
				_env = env;
				_jobjectAllocatedEvaluator = jallocatedEvaluator;
				_jclassAllocatedEvaluator = _env->GetObjectClass (_jobjectAllocatedEvaluator);
				_jmidSubmitContextAndTask = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextAndTaskString", "(Ljava/lang/String;Ljava/lang/String;)V");	
				_jmidSubmitContext = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextString", "(Ljava/lang/String;)V");

				fprintf(stdout, "AllocatedEvaluatorClr2Java _env %p\n", _env); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jclassAllocatedEvaluator %p\n", _jclassAllocatedEvaluator); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectAllocatedEvaluator %p\n", _jobjectAllocatedEvaluator); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContextAndTask %p\n", _jmidSubmitContextAndTask); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContext %p\n", _jmidSubmitContextAndTask); fflush (stdout);
			}

			void AllocatedEvaluatorClr2Java::SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
			{
				if(_jobjectAllocatedEvaluator == NULL)
				{
					fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
					return;
				}
				if(_jmidSubmitContextAndTask == NULL)
				{
					fprintf(stdout, " _jmidSubmitContextAndTask is NULL\n"); fflush (stdout);
					return;
				}
				_env -> CallObjectMethod(
					_jobjectAllocatedEvaluator, 
					_jmidSubmitContextAndTask, 
					JavaStringFromManagedString(_env, contextConfigStr), 
					JavaStringFromManagedString(_env, taskConfigStr));
			}

			void AllocatedEvaluatorClr2Java::SubmitContext(String^ contextConfigStr)
			{
				if(_jobjectAllocatedEvaluator == NULL)
				{
					fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
					return;
				}
				if(_jmidSubmitContext == NULL)
				{
					fprintf(stdout, " _jmidSubmitContext is NULL\n"); fflush (stdout);
					return;
				}
				_env -> CallObjectMethod(
					_jobjectAllocatedEvaluator, 
					_jmidSubmitContext, 
					JavaStringFromManagedString(_env, contextConfigStr));
			}
		}
	}
}