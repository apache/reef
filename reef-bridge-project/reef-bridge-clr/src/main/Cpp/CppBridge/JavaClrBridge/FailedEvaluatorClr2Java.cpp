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

					jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
					jfieldID jidEvaluatorId = env->GetFieldID(jclassFailedEvaluator, "evaluatorId", "Ljava/lang/String;");
					_jstringId = (jstring)env->GetObjectField(_jobjectFailedEvaluator, jidEvaluatorId);
					_jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringId));
				}

				IEvaluatorRequestorClr2Java^ FailedEvaluatorClr2Java::GetEvaluatorRequestor()
				{
					fprintf(stdout, "FailedEvaluatorClr2Java::GetEvaluatorRequestor\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
					jfieldID jidEvaluatorRequestor = env->GetFieldID(jclassFailedEvaluator, "evaluatorRequestorBridge", "Lcom/microsoft/reef/javabridge/EvaluatorRequestorBridge;");
					jobject jobjectEvaluatorRequestor = env->GetObjectField(_jobjectFailedEvaluator, jidEvaluatorRequestor);

					fprintf(stdout, "FailedEvaluatorClr2Java jidEvaluatorRequestor %p\n", jidEvaluatorRequestor); fflush (stdout);
					fprintf(stdout, "FailedEvaluatorClr2Java jobjectEvaluatorRequestor %p\n", jobjectEvaluatorRequestor); fflush (stdout);
					return gcnew EvaluatorRequestorClr2Java(env, jobjectEvaluatorRequestor);
				}

				String^ FailedEvaluatorClr2Java::GetId()
				{
					fprintf(stdout, "FailedEvaluatorClr2Java::GetId\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringId);
				}

				void FailedEvaluatorClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "FailedEvaluatorClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectFailedEvaluator);
				}
			}
		}
	}
}