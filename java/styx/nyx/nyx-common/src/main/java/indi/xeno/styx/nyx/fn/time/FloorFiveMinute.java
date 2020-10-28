package indi.xeno.styx.nyx.fn.time;

public class FloorFiveMinute extends FloorMinute {

  public FloorFiveMinute() {
    super(5);
  }

  @Override
  protected final String getFuncName() {
    return "floor_five_minute";
  }
}
