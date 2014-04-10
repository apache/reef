#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor)
				{
					_env = env;
					_jobjectEvaluatorRequestor = jevaluatorRequestor;
					_jclassEvaluatorRequestor = _env->GetObjectClass (_jobjectEvaluatorRequestor);
					_jmidSubmit = _env->GetMethodID(_jclassEvaluatorRequestor, "submit", "(II)V");

					fprintf(stdout, "EvaluatorRequestorClr2Java _env %p\n", _env); fflush (stdout);
					fprintf(stdout, "EvaluatorRequestorClr2Java _jobjectEvaluatorRequestor %p\n", _jobjectEvaluatorRequestor); fflush (stdout);
					fprintf(stdout, "EvaluatorRequestorClr2Java _jclassEvaluatorRequestor %p\n", _jclassEvaluatorRequestor); fflush (stdout);
					fprintf(stdout, "EvaluatorRequestorClr2Java _jmidSubmit %p\n", _jmidSubmit); fflush (stdout);
				}

				void EvaluatorRequestorClr2Java::Submit(EvaluatorRequest^ request)
				{
					if(_jobjectEvaluatorRequestor == NULL)
					{
						fprintf(stdout, " _jobjectEvaluatorRequestor is NULL\n"); fflush (stdout);
						return;
					}
					if(_jmidSubmit == NULL)
					{
						fprintf(stdout, " _jmidSubmit is NULL\n"); fflush (stdout);
						return;
					}
					_env -> CallObjectMethod(
						_jobjectEvaluatorRequestor, 
						_jmidSubmit, 
						request -> Number,
						request -> MemoryMegaBytes);
				}
			}
		}
	}
}