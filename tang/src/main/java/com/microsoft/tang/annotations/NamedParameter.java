package com.microsoft.tang.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedParameter {
  //Class<?> type() default String.class;
  String doc() default "";
  String default_value() default "";
  String short_name() default "";
}
