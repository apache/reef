package com.microsoft.canberra.math;

import java.util.ArrayList;
import java.util.List;


// TODO: Document
public class Window {
  private final int maxSize;
  private final List<Double> list;

  public Window(int size) {
    this.maxSize = size;
    list = new ArrayList<>(size);
  }

  public void add(double d) {
    if (list.size() < maxSize) {
      list.add(d);
      return;
    }
    list.remove(0);
    list.add(d);
  }

  public double avg() {
    if (list.size() == 0)
      return 0;
    double retVal = 0;
    for (double d : list) {
      retVal += d;
    }
    return retVal / list.size();
  }

  public double avgIfAdded(double d) {
    if (list.isEmpty())
      return d;
    int start = (list.size() < maxSize) ? 0 : 1;
    int numElems = (list.size() < maxSize) ? list.size() + 1 : maxSize;
    for (int i = start; i < list.size(); i++)
      d += list.get(i);
    return d / numElems;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Window w = new Window(3);
    for (int i = 1; i < 10; i++) {
      double exp = w.avgIfAdded(i);
      w.add(i);
      double act = w.avg();
      System.out.println("Exp: " + exp + " Act: " + act);
    }

  }

}
