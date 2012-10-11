package com.microsoft.inject;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({})
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedParameter {
	String value();
	String type() default "java.lang.String";
	String doc() default "";
	String default_value() default "";
}
