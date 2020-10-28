package indi.xeno.styx.nyx.fn.time;

import indi.xeno.styx.nyx.fn.EvalFunc;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.Optional;

import static indi.xeno.styx.charon.util.TimeUtils.floorMonth;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

abstract class FloorMonth extends BaseTimeFn {

  private final IntWritable output;

  private transient EvalFunc<Optional<Integer>> monthFunc;

  private final transient int step;

  FloorMonth(int step) {
    super(1);
    output = new IntWritable();
    monthFunc = EMPTY_FUNC;
    this.step = step;
  }

  @Override
  public final ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    checkArgSizeAndAllPrimitive(arguments);
    monthFunc = initYearMonthAtFirst(arguments, HiveIntervalYearMonth::getMonths, Date::getMonth);
    return writableIntObjectInspector;
  }

  @Override
  public final IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
    Optional<Integer> month = monthFunc.apply(arguments);
    if (month.isEmpty()) {
      return null;
    }
    output.set(floorMonth(month.get(), step));
    return output;
  }
}
