#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				HttpServerClr2Java::HttpServerClr2Java(JNIEnv *env, jlong handle, jobject jhttpServerEventBridge, jobject jlogger)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_handle = handle;
					_jhttpServerEventBridge = reinterpret_cast<jobject>(env->NewGlobalRef(jhttpServerEventBridge));
					_jlogger = reinterpret_cast<jobject>(env->NewGlobalRef(jlogger));

					fprintf(stdout, "HttpServerClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java _jhttpServerEventBridge %p\n", _jhttpServerEventBridge); fflush (stdout);
					
				}

				String^ HttpServerClr2Java::GetQueryString()				
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerNRTEventBridge = env->GetObjectClass (_jhttpServerEventBridge);
					jmethodID jmidgetQueryString = env->GetMethodID(jclasshttpServerNRTEventBridge, "getQueryString", "()Ljava/lang/String;");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerNRTEventBridge %p\n", jclasshttpServerNRTEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidgetQueryString %p\n", jmidgetQueryString); fflush (stdout);

					if(jmidgetQueryString == NULL)
					{
						fprintf(stdout, " jmidgetQueryString is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jQueryString = (jstring) env->CallObjectMethod(
						_jhttpServerEventBridge, 
						jmidgetQueryString);
					
					String^ queryString = ManagedStringFromJavaString(env, jQueryString);

					return queryString;
				}

				void HttpServerClr2Java::SetQueryResult(String^ queryResult)
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerEventBridge = env->GetObjectClass (_jhttpServerEventBridge);
					jmethodID jmidsetQueryResult = env->GetMethodID(jclasshttpServerEventBridge, "setQueryResult", "(Ljava/lang/String;)V");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerEventBridge %p\n", jclasshttpServerEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidsetQueryResult %p\n", jmidsetQueryResult); fflush (stdout);

					if(jmidsetQueryResult == NULL)
					{
						fprintf(stdout, " jmidsetQueryResult is NULL\n"); fflush (stdout);
						return;
					}
					env->CallObjectMethod(
						_jhttpServerEventBridge, 
						jmidsetQueryResult,
						JavaStringFromManagedString(env, queryResult));					

				}	

				void HttpServerClr2Java::SetUriSpecification(String^ uriSpecification)
				{					
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclasshttpServerEventBridge = env->GetObjectClass (_jhttpServerEventBridge);
					jmethodID jmidsetUriSpecification = env->GetMethodID(jclasshttpServerEventBridge, "setUriSpecification", "(Ljava/lang/String;)V");

					fprintf(stdout, "HttpServerClr2Java jclasshttpServerEventBridge %p\n", jclasshttpServerEventBridge); fflush (stdout);
					fprintf(stdout, "HttpServerClr2Java jmidsetUriSpecification %p\n", jmidsetUriSpecification); fflush (stdout);

					if(jmidsetUriSpecification == NULL)
					{
						fprintf(stdout, " jmidsetUriSpecification is NULL\n"); fflush (stdout);
						return;
					}
					env->CallObjectMethod(
						_jhttpServerEventBridge, 
						jmidsetUriSpecification,
						JavaStringFromManagedString(env, uriSpecification));					

				}				
			}
		}
	}
}