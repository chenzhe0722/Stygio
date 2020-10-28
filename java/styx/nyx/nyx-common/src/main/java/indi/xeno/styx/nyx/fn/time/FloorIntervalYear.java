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

import static indi.xeno.styx.charon.util.TimeUtils.floorYear;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

public class FloorIntervalYear extends BaseTimeFn {

  private final IntWritable output;

  private transient EvalFunc<Optional<Integer>> yearFunc;

  private transient EvalFunc<Optional<Integer>> stepFunc;

  private transient EvalFunc<Optional<Integer>> offsetFunc;

  public FloorIntervalYear() {
    super(3);
    output = new IntWritable();
    yearFunc = EMPTY_FUNC;
    stepFunc = EMPTY_FUNC;
    offsetFunc = EMPTY_FUNC;
  }

  @Override
  public final ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    checkArgSizeAndAllPrimitive(arguments);
    yearFunc = initYearMonthAtFirst(arguments, HiveIntervalYearMonth::getYears, Date::getYear);
    stepFunc = initIntValueAndCheckConst(arguments, 1);
    offsetFunc = initIntValueAndCheckConst(arguments, 2);
    return writableIntObjectInspector;
  }

  @Override
  public final IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
    Optional<Integer> year = yearFunc.apply(arguments);
    if (year.isEmpty()) {
      return null;
    }
    Optional<Integer> step = stepFunc.apply(arguments);
    if (step.isEmpty()) {
      return null;
    }
    Optional<Integer> offset = offsetFunc.apply(arguments);
    if (offset.isEmpty()) {
      return null;
    }
    output.set(floorYear(year.get(), step.get(), offset.get()));
    return output;
  }

  @Override
  protected final String getFuncName() {
    return "floor_interval_year";
  }
}
