package indi.xeno.styx.nyx.fn.time;

public class FloorTenMinute extends FloorMinute {

  public FloorTenMinute() {
    super(10);
  }

  @Override
  protected final String getFuncName() {
    return "floor_ten_minute";
  }
}
