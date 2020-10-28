package indi.xeno.styx.nyx.fn.time;

import indi.xeno.styx.nyx.fn.EvalFunc;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.Optional;

import static indi.xeno.styx.charon.util.TimeUtils.floorTime;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

abstract class FloorMinute extends BaseTimeFn {

  private final IntWritable output;

  private transient EvalFunc<Optional<Integer>> minuteFunc;

  private final transient int step;

  FloorMinute(int step) {
    super(1);
    output = new IntWritable();
    minuteFunc = EMPTY_FUNC;
    this.step = step;
  }

  @Override
  public final ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    checkArgSizeAndAllPrimitive(arguments);
    minuteFunc = initTimeAtFirst(arguments, HiveIntervalDayTime::getMinutes, Timestamp::getMinutes);
    return writableIntObjectInspector;
  }

  @Override
  public final IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
    Optional<Integer> minute = minuteFunc.apply(arguments);
    if (minute.isEmpty()) {
      return null;
    }
    output.set(floorTime(minute.get(), step));
    return output;
  }
}
