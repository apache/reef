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
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectEvaluatorRequestor = reinterpret_cast<jobject>(env->NewGlobalRef(jevaluatorRequestor));

					fprintf(stdout, "EvaluatorRequestorClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "EvaluatorRequestorClr2Java _jobjectEvaluatorRequestor %p\n", _jobjectEvaluatorRequestor); fflush (stdout);
				}

				void EvaluatorRequestorClr2Java::Submit(IEvaluatorRequest^ request)
				{
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclassEvaluatorRequestor = env->GetObjectClass (_jobjectEvaluatorRequestor);
					jmethodID jmidSubmit = env->GetMethodID(jclassEvaluatorRequestor, "submit", "(IILjava/lang/String;)V");

					fprintf(stdout, "EvaluatorRequestorClr2Java jclassEvaluatorRequestor %p\n", jclassEvaluatorRequestor); fflush (stdout);
					fprintf(stdout, "EvaluatorRequestorClr2Java jmidSubmit %p\n", jmidSubmit); fflush (stdout);

					if(jmidSubmit == NULL)
					{
						fprintf(stdout, " jmidSubmit is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectEvaluatorRequestor, 
						jmidSubmit, 
						request -> Number,
						request -> MemoryMegaBytes,
						JavaStringFromManagedString(env, request -> Rack));
				}

				void EvaluatorRequestorClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "EvaluatorRequestorClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectEvaluatorRequestor);
				}
			}
		}
	}
}