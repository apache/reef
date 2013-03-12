package com.microsoft.tang.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A TANG Unit consists of an outer class and some non-static inner classes.
 * TANG injectors automatically treat all the classes in a unit as singletons.
 * 
 * In order to inject the singleton instance of each inner class, TANG first
 * instantiates the outer class and then uses the resulting instance to
 * instantiate each inner class.
 * 
 * Classes annotated in this way must have at least one non-static inner class
 * and no static inner classes. The inner classes must not declare any
 * constructors.
 * 
 * Furthermore, classes annotated with Unit may not have injectable (or Unit)
 * subclasses.
 * 
 * @author sears
 * 
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unit {

}
