package indi.xeno.styx.nyx.fn.time;

public class FloorTwelveHour extends FloorHour {

  public FloorTwelveHour() {
    super(12);
  }

  @Override
  protected final String getFuncName() {
    return "floor_twelve_hour";
  }
}
