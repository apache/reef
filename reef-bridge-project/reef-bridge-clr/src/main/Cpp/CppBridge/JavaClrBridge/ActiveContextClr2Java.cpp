#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				ActiveContextClr2Java::ActiveContextClr2Java(JNIEnv *env, jobject jobjectActiveContext)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectActiveContext = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectActiveContext));

					jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
					jfieldID jidContextId = env->GetFieldID(jclassActiveContext, "contextId", "Ljava/lang/String;");
					_jstringId = (jstring)env->GetObjectField(_jobjectActiveContext, jidContextId);
					_jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringId));

					jfieldID jidEvaluatorId = env->GetFieldID(jclassActiveContext, "evaluatorId", "Ljava/lang/String;");
					_jstringEvaluatorId = (jstring)env->GetObjectField(_jobjectActiveContext, jidEvaluatorId);
					_jstringEvaluatorId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringEvaluatorId));

					fprintf(stdout, "ActiveContextClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jobjectActiveContext %p\n", _jobjectActiveContext); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jstringId %p\n", _jstringId); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java _jstringEvaluatorId %p\n", _jstringEvaluatorId); fflush (stdout);
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

				void ActiveContextClr2Java::OnError(String^ message)
				{
					fprintf(stdout, "ActiveContextClr2Java::OnError\n"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectActiveContext);
				}

				void ActiveContextClr2Java::Close()
				{
					fprintf(stdout, "ActiveContextClr2Java::Close"); fflush (stdout);					
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassActiveContext = env->GetObjectClass (_jobjectActiveContext);
					jmethodID jmidClose = env->GetMethodID(jclassActiveContext, "close", "()V");

					fprintf(stdout, "ActiveContextClr2Java jclassActiveContext %p\n", jclassActiveContext); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java jmidClose %p\n", jmidClose); fflush (stdout);

					if(jmidClose == NULL)
					{
						fprintf(stdout, " jmidClose is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectActiveContext, 
						jmidClose);
				}

				String^ ActiveContextClr2Java::GetId()
				{
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringId);
				}

				String^ ActiveContextClr2Java::GetEvaluatorId()
				{
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringEvaluatorId);
				}

				IEvaluatorDescriptor^ ActiveContextClr2Java::GetEvaluatorDescriptor()
				{
					fprintf(stdout, "ActiveContextClr2Java::GetEvaluatorDescriptor"); fflush (stdout);															
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassActiveContext = env->GetObjectClass (_jobjectActiveContext);
					jmethodID jmidGetEvaluatorDescriptor= env->GetMethodID(jclassActiveContext, "getEvaluatorDescriptorSring", "()Ljava/lang/String;");	

					fprintf(stdout, "ActiveContextClr2Java jclassActiveContext %p\n", jclassActiveContext); fflush (stdout);
					fprintf(stdout, "ActiveContextClr2Java jmidGetEvaluatorDescriptor %p\n", jmidGetEvaluatorDescriptor); fflush (stdout);

					if(jmidGetEvaluatorDescriptor == NULL)
					{
						fprintf(stdout, " jmidGetEvaluatorDescriptor is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jevaluatorDescriptorString = (jstring)env -> CallObjectMethod(
						_jobjectActiveContext, 
						jmidGetEvaluatorDescriptor);
					String^ evaluatorDescriptorString = ManagedStringFromJavaString(env, jevaluatorDescriptorString);
					EvaluatorDescriptorImpl^ descriptor = gcnew EvaluatorDescriptorImpl(evaluatorDescriptorString);
					return descriptor;
				}
			}
		}
	}
}