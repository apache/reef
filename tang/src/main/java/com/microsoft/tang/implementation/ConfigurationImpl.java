package com.microsoft.tang.implementation;

import com.microsoft.tang.Configuration;

public class ConfigurationImpl implements Configuration {
  public final ConfigurationBuilderImpl builder;

  public ConfigurationImpl(ConfigurationBuilderImpl builder) {
    this.builder = builder;
  }

}