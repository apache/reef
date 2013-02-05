package com.microsoft.tang.implementation.java;

import com.microsoft.tang.Configuration;

public class ConfigurationImpl implements Configuration {
  public final ConfigurationBuilderImpl builder;

  ConfigurationImpl(ConfigurationBuilderImpl builder) {
    this.builder = builder;
  }

}