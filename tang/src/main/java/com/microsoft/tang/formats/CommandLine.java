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
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.ReflectionUtilities;

public class CommandLine {
  private final ConfigurationBuilder conf;
  private final Map<String,String> shortNames = new MonotonicMap<>();

  public CommandLine(ConfigurationBuilder conf) {
    this.conf = conf;
  }
  public void registerShortNameOfClass(String s) throws BindException {
    final Node n;
    try {
      n = conf.getClassHierarchy().getNode(s);
    } catch(NameResolutionException e) {
      throw new BindException("Problem loading class " + s, e);
    }
    if(n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>)n;
      String shortName = np.getShortName();
      String longName = np.getFullName();
      shortNames.put(shortName, longName);
    } else {
      throw new BindException("Can't register short name for non-NamedParameterNode: " + n);
    }
  }
  public void registerShortNameOfClass(Class<?> c) throws BindException {
    registerShortNameOfClass(ReflectionUtilities.getFullName(c));
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
    for (String shortName : shortNames.keySet()) {
      String longName = shortNames.get(shortName);
      try {
        opts.addOption(OptionBuilder
            .withArgName(conf.classPrettyDefaultString(longName)).hasArg()
            .withDescription(conf.classPrettyDescriptionString(longName))
            .create(shortName));
      } catch (BindException e) {
        throw new IllegalStateException(
            "Could not process " + shortName + " which is the short name of " + longName, e);
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
          conf.bind(shortNames.get(shortName), value);
        } catch (ClassNotFoundException e) {
          throw new BindException("Could not bind shortName " + shortName + " to value " + value, e);
        }
      }
    }
    return true;
  }

}
