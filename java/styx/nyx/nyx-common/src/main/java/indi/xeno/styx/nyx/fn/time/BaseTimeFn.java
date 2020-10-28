package indi.xeno.styx.nyx.fn.time;

import indi.xeno.styx.nyx.fn.BaseFn;
import indi.xeno.styx.nyx.fn.EvalFunc;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_DAY_TIME;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_YEAR_MONTH;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;

abstract class BaseTimeFn extends BaseFn {

  private final transient Converter[] converters;

  private final transient PrimitiveCategory[] inputTypes;

  static final EvalFunc<Optional<Integer>> EMPTY_FN = args -> empty();

  BaseTimeFn(int len) {
    converters = new Converter[len];
    inputTypes = new PrimitiveCategory[len];
  }

  final void checkArgSizeAndAllPrimitive(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    checkArgsSize(arguments, converters.length, converters.length);
    for (int i = 0; i < converters.length; i++) {
      checkArgPrimitive(arguments, i);
    }
  }

  final EvalFunc<Optional<Integer>> initYearMonthAtFirst(
      ObjectInspector[] arguments,
      Function<HiveIntervalYearMonth, Integer> intervalFn,
      Function<Date, Integer> dateFn)
      throws UDFArgumentTypeException {
    PrimitiveObjectInspector time = (PrimitiveObjectInspector) arguments[0];
    switch (time.getPrimitiveCategory()) {
      case INTERVAL_YEAR_MONTH:
        return initIntervalYearMonthValueAtFirst(arguments)
            .andThen(interval -> interval.map(intervalFn));
      case STRING:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
      case VOID:
        return initDateValueAtFirst(arguments).andThen(date -> date.map(dateFn));
      default:
        throw new UDFArgumentTypeException(
            0, getFuncName() + " does not take " + time.getPrimitiveCategory() + " type");
    }
  }

  final EvalFunc<Optional<Integer>> initTimeAtFirst(
      ObjectInspector[] arguments,
      Function<HiveIntervalDayTime, Integer> intervalFn,
      Function<Timestamp, Integer> timestampFn)
      throws UDFArgumentTypeException {
    PrimitiveObjectInspector time = (PrimitiveObjectInspector) arguments[0];
    switch (time.getPrimitiveCategory()) {
      case INTERVAL_DAY_TIME:
        return initIntervalDayTimeValueAtFirst(arguments)
            .andThen(interval -> interval.map(intervalFn));
      case STRING:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
      case VOID:
        return initTimestampValueAtFirst(arguments)
            .andThen(timestamp -> timestamp.map(timestampFn));
      default:
        throw new UDFArgumentTypeException(
            0, getFuncName() + " does not take " + time.getPrimitiveCategory() + " type");
    }
  }

  private EvalFunc<Optional<HiveIntervalYearMonth>> initIntervalYearMonthValueAtFirst(
      ObjectInspector[] arguments) {
    inputTypes[0] = INTERVAL_YEAR_MONTH;
    converters[0] = getConverter(arguments[0], writableHiveIntervalYearMonthObjectInspector);
    return args -> ofNullable(getIntervalYearMonthValue(args, 0, inputTypes, converters));
  }

  private EvalFunc<Optional<HiveIntervalDayTime>> initIntervalDayTimeValueAtFirst(
      ObjectInspector[] arguments) {
    inputTypes[0] = INTERVAL_DAY_TIME;
    converters[0] = getConverter(arguments[0], writableHiveIntervalDayTimeObjectInspector);
    return args -> ofNullable(getIntervalDayTimeValue(args, 0, inputTypes, converters));
  }

  private EvalFunc<Optional<Date>> initDateValueAtFirst(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {
    obtainDateConverter(arguments, 0, inputTypes, converters);
    return args -> ofNullable(getDateValue(args, 0, inputTypes, converters));
  }

  private EvalFunc<Optional<Timestamp>> initTimestampValueAtFirst(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {
    obtainTimestampConverter(arguments, 0, inputTypes, converters);
    return args -> ofNullable(getTimestampValue(args, 0, converters));
  }

  final EvalFunc<Optional<Integer>> initIntValueAndCheckConst(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    checkArgGroups(arguments, i, inputTypes, NUMERIC_GROUP);
    obtainIntConverter(arguments, i, inputTypes, converters);
    if (arguments[i] instanceof ConstantObjectInspector) {
      Optional<Integer> constant = ofNullable(getConstantIntValue(arguments, i));
      return args -> constant;
    }
    return args -> ofNullable(getIntValue(args, i, converters));
  }
}
