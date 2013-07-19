package com.microsoft.tang.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows interfaces to specify a default implementation.
 * 
 * Note that the default can be overridden after the fact
 * by explicitly binding a different implementation to the
 * interface.
 * 
 * For "normal" injections of a given library, this reduces
 * the amount of boilerplate configuration code needed,
 * and also shrinks the Tang configuration objects that
 * need to be passed around.
 * 
 */
@Target(ElementType.TYPE)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultImplementation {
  Class<?> value() default Void.class;
  String name() default "";
}
