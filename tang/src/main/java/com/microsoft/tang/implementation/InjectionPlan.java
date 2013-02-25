package com.microsoft.tang.implementation;

import com.microsoft.tang.types.Node;

public abstract class InjectionPlan<T> {
  final Node node;

  public InjectionPlan(Node node) {
    this.node = node;
  }

  public Node getNode() {
    return node;
  }

  public abstract int getNumAlternatives();

  public boolean isFeasible() {
    return getNumAlternatives() > 0;
  }

  abstract public boolean isAmbiguous();

  abstract public boolean isInjectable();

  private static void newline(StringBuffer pretty, int indent) {
    pretty.append('\n');
    for (int j = 0; j < indent * 3; j++) {
      pretty.append(' ');
    }
  }

  public String toPrettyString() {
    String ugly = toString();
    StringBuffer pretty = new StringBuffer();
    int currentIndent = 0;
    for (int i = 0; i < ugly.length(); i++) {
      char c = ugly.charAt(i);
      if (c == '[') {
        currentIndent++;
        pretty.append(c);
        newline(pretty, currentIndent);
      } else if (c == ']') {
        currentIndent--;
        pretty.append(c);
        // newline(pretty, currentIndent);
      } else if (c == '|') {
        newline(pretty, currentIndent);
        pretty.append(c);
      } else {
        pretty.append(c);
      }
    }
    return pretty.toString();
  }
}
