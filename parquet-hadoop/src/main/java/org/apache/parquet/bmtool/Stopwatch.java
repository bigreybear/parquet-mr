package org.apache.parquet.bmtool;

public class Stopwatch {
  static long acc, temp;
  public static void zero() {
    acc  = temp = 0;
  }

  public static void start() {
    temp = System.nanoTime();
  }

  public static void stop() {
    acc += System.nanoTime() - temp;
  }

  public static long report() {
    return acc;
  }

  public static long reportMilSecs() {
    return acc/ 1000000;
  }
}
