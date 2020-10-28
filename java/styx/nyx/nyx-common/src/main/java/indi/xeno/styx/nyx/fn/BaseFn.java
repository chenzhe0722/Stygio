package indi.xeno.styx.nyx.fn;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public abstract class BaseFn extends GenericUDF {

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }
}
