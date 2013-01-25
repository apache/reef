package com.microsoft.tang.formats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.exceptions.BindException;

public class CommandLine {
  private final ConfigurationBuilder conf;

  public CommandLine(ConfigurationBuilder conf) {
    this.conf = conf;
  }

  /**
   * @param option
   * @param cb
   */
  @SuppressWarnings("static-access")
  private Options getCommandLineOptions() {
    Options opts = new Options();
    // Collection<NamedParameterNode<?>> namedParameters = conf.getNamespace()
    // .getNamedParameterNodes();
    // for (NamedParameterNode<?> param : namedParameters) {
    // String shortName = param.getShortName();
    for (String shortName : conf.getShortNames()) {
      if (shortName != null) {
        try {
          String longName = conf.resolveShortName(shortName);
          opts.addOption(OptionBuilder
              .withArgName(conf.classPrettyDefaultString(longName)).hasArg()
              .withDescription(conf.classPrettyDescriptionString(longName))
              .create(shortName));
        } catch (BindException e) {
          throw new IllegalStateException(
              "Configuration object mentioned short name " + shortName
                  + " and then did not recognize it!", e);
        }
      }
    }
    for (Option o : applicationOptions.keySet()) {
      opts.addOption(o);
    }
    return opts;
  }

  public interface CommandLineCallback {
    public void process(Option option);
  }

  Map<Option, CommandLineCallback> applicationOptions = new HashMap<Option, CommandLineCallback>();

  public void addCommandLineOption(Option option, CommandLineCallback cb) {
    // TODO: Check for conflicting options.
    applicationOptions.put(option, cb);
  }

  /**
   * @return true if the command line parsing succeeded, false (or exception)
   *         otherwise.
   * @param args
   * @throws IOException
   * @throws NumberFormatException
   * @throws ParseException
   */
  public <T> boolean processCommandLine(String[] args) throws IOException,
      BindException {
    Options o = getCommandLineOptions();
    Option helpFlag = new Option("?", "help");
    o.addOption(helpFlag);
    Parser g = new GnuParser();
    org.apache.commons.cli.CommandLine cl;
    try {
      cl = g.parse(o, args);
    } catch (ParseException e) {
      throw new IOException("Could not parse config file", e);
    }
    if (cl.hasOption("?")) {
      HelpFormatter help = new HelpFormatter();
      help.printHelp("reef", o);
      return false;
    }
    for (Option option : cl.getOptions()) {
      String shortName = option.getOpt();
      String value = option.getValue();

      if (applicationOptions.containsKey(option)) {
        applicationOptions.get(option).process(option);
      } else {
        try {
          conf.bind(conf.resolveShortName(shortName), value);
        } catch (ClassNotFoundException e) {
          throw new BindException("Could not bind shortName " + shortName + " to value " + value, e);
        }
      }
    }
    return true;
  }

}
