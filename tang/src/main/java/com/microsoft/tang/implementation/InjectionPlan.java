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
    for (int j = 0; j < indent * 2; j++) {
      pretty.append(' ');
    }
  }

  public String toPrettyString() {
    String ugly = node.getFullName() + ":\n" + toString();
    StringBuffer pretty = new StringBuffer();
    int currentIndent = 1;
    for (int i = 0; i < ugly.length(); i++) {
      char c = ugly.charAt(i);
      if (c == '(') {
        if(ugly.charAt(i+1) == ')') {
          pretty.append("()");
          i++;
        } else {
          newline(pretty, currentIndent);
          currentIndent++;
          pretty.append(c);
          pretty.append(' ');
        }
      } else if (c == '[') {
          if(ugly.charAt(i+1) == ']') {
            pretty.append("[]");
            i++;
          } else {
            newline(pretty, currentIndent);
            currentIndent++;
            pretty.append(c);
            pretty.append(' ');
          }
      } else if (c == ')' || c == ']') {
        currentIndent--;
        newline(pretty, currentIndent);
        pretty.append(c);
      } else if (c == '|') {
        newline(pretty, currentIndent);
        pretty.append(c);
      } else if (c == ',') {
        currentIndent--;
        newline(pretty, currentIndent);
        pretty.append(c);
        currentIndent++;
      } else {
        pretty.append(c);
      }
    }
    return pretty.toString();
  }
}
