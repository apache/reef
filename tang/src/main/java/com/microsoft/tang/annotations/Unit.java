package com.microsoft.tang.annotations;

/**
 * A TANG Unit consists of an outer class and some non-static inner classes.
 * TANG injectors automatically treat all the classes in a unit as singletons.
 * 
 * In order to inject the singleton instance of each inner class, TANG first
 * instantiates the outer class, and then uses the resulting instance to
 * instantiate each inner class.
 * 
 * Classes annotated with this target must have at least one non-static inner
 * class and no static inner classes. The inner classes must not declare any
 * constructors.
 * 
 * @author sears
 * 
 */
public @interface Unit {

}
