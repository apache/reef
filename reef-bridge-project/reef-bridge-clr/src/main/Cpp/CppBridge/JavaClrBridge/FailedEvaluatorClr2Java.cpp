#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				FailedEvaluatorClr2Java::FailedEvaluatorClr2Java(JNIEnv *env, jobject jobjectFailedEvaluator)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectFailedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedEvaluator));

					fprintf(stdout, "FailedEvaluatorClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "FailedEvaluatorClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "FailedEvaluatorClr2Java _jobjectFailedEvaluator %p\n", _jobjectFailedEvaluator); fflush (stdout);
				}

				IEvaluatorRequestorClr2Java^ FailedEvaluatorClr2Java::GetEvaluatorRequestor()
				{
					fprintf(stdout, "FailedEvaluatorClr2Java::GetEvaluatorRequestor"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
					jfieldID jidEvaluatorRequestor = env->GetFieldID(jclassFailedEvaluator, "evaluatorRequestorBridge", "Ljavabridge/EvaluatorRequestorBridge;");
					jobject jobjectEvaluatorRequestor = env->GetObjectField(_jobjectFailedEvaluator, jidEvaluatorRequestor);

					fprintf(stdout, "FailedEvaluatorClr2Java jidEvaluatorRequestor %p\n", jidEvaluatorRequestor); fflush (stdout);
					fprintf(stdout, "FailedEvaluatorClr2Java jobjectEvaluatorRequestor %p\n", jobjectEvaluatorRequestor); fflush (stdout);
					return gcnew EvaluatorRequestorClr2Java(env, jobjectEvaluatorRequestor);
				}
			}
		}
	}
}