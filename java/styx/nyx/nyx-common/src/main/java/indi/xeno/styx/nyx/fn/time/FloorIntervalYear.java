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

  private transient EvalFunc<Optional<Integer>> yearFn;

  private transient EvalFunc<Optional<Integer>> stepFn;

  private transient EvalFunc<Optional<Integer>> offsetFn;

  public FloorIntervalYear() {
    super(3);
    output = new IntWritable();
    yearFn = EMPTY_FN;
    stepFn = EMPTY_FN;
    offsetFn = EMPTY_FN;
  }

  @Override
  public final ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    checkArgSizeAndAllPrimitive(arguments);
    yearFn = initYearMonthAtFirst(arguments, HiveIntervalYearMonth::getYears, Date::getYear);
    stepFn = initIntValueAndCheckConst(arguments, 1);
    offsetFn = initIntValueAndCheckConst(arguments, 2);
    return writableIntObjectInspector;
  }

  @Override
  public final IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
    Optional<Integer> year = yearFn.apply(arguments);
    if (year.isEmpty()) {
      return null;
    }
    Optional<Integer> step = stepFn.apply(arguments);
    if (step.isEmpty()) {
      return null;
    }
    Optional<Integer> offset = offsetFn.apply(arguments);
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
