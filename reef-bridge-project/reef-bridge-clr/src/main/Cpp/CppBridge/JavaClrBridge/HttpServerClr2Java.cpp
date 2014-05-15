#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				HttpServerClr2Java::HttpServerClr2Java(JNIEnv *env, jlong handle, jobject jhttpServerNRTEventBridge, jobject jlogger)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_handle = handle;
					_jhttpServerNRTEventBridge = reinterpret_cast<jobject>(env->NewGlobalRef(jhttpServerNRTEventBridge));
					_jlogger = reinterpret_cast<jobject>(env->NewGlobalRef(jlogger));

					fprintf(stdout, "HttpServerClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java _jhttpServerNRTEventBridge %p\n", _jhttpServerNRTEventBridge); fflush (stdout);
					
				}

				String^ HttpServerClr2Java::GetQueryString()				
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerNRTEventBridge = env->GetObjectClass (_jhttpServerNRTEventBridge);
					jmethodID jmidgetQueryString = env->GetMethodID(jclasshttpServerNRTEventBridge, "getQueryString", "()Ljava/lang/String;");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerNRTEventBridge %p\n", jclasshttpServerNRTEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidgetQueryString %p\n", jmidgetQueryString); fflush (stdout);

					if(jmidgetQueryString == NULL)
					{
						fprintf(stdout, " jmidgetQueryString is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jQueryString = (jstring) env->CallObjectMethod(
						_jhttpServerNRTEventBridge, 
						jmidgetQueryString);
					
					String^ queryString = ManagedStringFromJavaString(env, jQueryString);

					return queryString;
				}

				String^ HttpServerClr2Java::GetQueryResult()
				{
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerNRTEventBridge = env->GetObjectClass (_jhttpServerNRTEventBridge);
					jmethodID jmidgetResultString = env->GetMethodID(jclasshttpServerNRTEventBridge, "getResultString", "()Ljava/lang/String;");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerNRTEventBridge %p\n", jclasshttpServerNRTEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidgetResultString %p\n", jmidgetResultString); fflush (stdout);

					if(jmidgetResultString == NULL)
					{
						fprintf(stdout, " jmidgetResultString is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jResultString = (jstring) env->CallObjectMethod(
						_jhttpServerNRTEventBridge, 
						jmidgetResultString);
					
					String^ resultString = ManagedStringFromJavaString(env, jResultString);

					return resultString;
				}

				void HttpServerClr2Java::SetQueryResult(String^ queryResult)
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerNRTEventBridge = env->GetObjectClass (_jhttpServerNRTEventBridge);
					jmethodID jmidsetQueryResult = env->GetMethodID(jclasshttpServerNRTEventBridge, "setQueryResult", "(Ljava/lang/String;)V");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerNRTEventBridge %p\n", jclasshttpServerNRTEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidsetQueryResult %p\n", jmidsetQueryResult); fflush (stdout);

					if(jmidsetQueryResult == NULL)
					{
						fprintf(stdout, " jmidsetQueryResult is NULL\n"); fflush (stdout);
						return;
					}
					env->CallObjectMethod(
						_jhttpServerNRTEventBridge, 
						jmidsetQueryResult,
						JavaStringFromManagedString(env, queryResult));					

				}
				
			}
		}
	}
}