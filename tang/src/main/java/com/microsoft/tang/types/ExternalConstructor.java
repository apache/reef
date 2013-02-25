package com.microsoft.tang.types;

/**
 * This interface allows legacy classes to be injected by
 * ConfigurationBuilderImpl. To be of any use, implementations of this class
 * must have at least one constructor with an @Inject annotation. From
 * ConfigurationBuilderImpl's perspective, an ExternalConstructor class is just
 * a special instance of the class T, except that, after injection an
 * ExternalConstructor, ConfigurationBuilderImpl will call newInstance, and
 * store the resulting object. It will then discard the ExternalConstructor.
 * 
 * @author sears
 * 
 * @param <T>
 *          The type this ExternalConstructor will create.
 */
public interface ExternalConstructor<T> {
  /**
   * This method will only be called once.
   * 
   * @return a new, distinct instance of T.
   */
  public T newInstance();
}
