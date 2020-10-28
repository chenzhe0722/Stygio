package indi.xeno.styx.charon.util;

public abstract class TimeUtils {

  private TimeUtils() {}

  public static int floorYear(int year, int step, int offset) {
    int rem = year % step;
    int base = year - rem + offset;
    if (rem >= offset) {
      return base;
    } else {
      return base - step;
    }
  }

  public static int floorMonth(int month, int step) {
    return month - (month - 1) % step;
  }

  public static int floorTime(int time, int step) {
    return time - time % step;
  }
}
