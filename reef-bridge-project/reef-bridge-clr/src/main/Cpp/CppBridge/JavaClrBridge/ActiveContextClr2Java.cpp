#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			ActiveContextClr2Java::ActiveContextClr2Java(JNIEnv *env, jobject jallocatedEvaluator)
			{
				_env = env;
				_jobjectActiveContext = jallocatedEvaluator;
				_jclassActiveContext = _env->GetObjectClass (_jobjectActiveContext);
				_jmidSubmitTask = _env->GetMethodID(_jclassActiveContext, "submitTaskString", "(Ljava/lang/String;)V");

				fprintf(stdout, "AllocatedEvaluatorClr2Java _env %p\n", _env); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectActiveContext %p\n", _jobjectActiveContext); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jclassActiveContext %p\n", _jclassActiveContext); fflush (stdout);
				fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitTask %p\n", _jmidSubmitTask); fflush (stdout);
			}

			void ActiveContextClr2Java::SubmitTask( String^ taskConfigStr)
			{
				if(_jobjectActiveContext == NULL)
				{
					fprintf(stdout, " _jobjectActiveContext is NULL\n"); fflush (stdout);
					return;
				}
				if(_jmidSubmitTask == NULL)
				{
					fprintf(stdout, " _jmidSubmitTask is NULL\n"); fflush (stdout);
					return;
				}
				_env -> CallObjectMethod(
					_jobjectActiveContext, 
					_jmidSubmitTask, 
					JavaStringFromManagedString(_env, taskConfigStr));
			}
		}
	}
}